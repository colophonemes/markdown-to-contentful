var contentful = require('contentful-management');
var minimatch = require('minimatch');
var debug = require('debug')('contentful-to-markdown');
var Metalsmith = require('metalsmith');
var Promise = require('bluebird');
var PromiseQueue = require('bluebird-queue')
var fs = require('fs');
var path = require('path');
var mime = require('mime');
var ProgressBar = require('progress')
var slug = require('slug'); slug.defaults.mode = 'rfc3986';
var unique = require('array-uniq');
var moment = require('moment');


try {
    var spaceID = require('./local.config').spaceID
    var accessToken = require('./local.config').accessToken
} catch (error) {
    console.error('Error, cannot connect to server, no credentials')
}


var client = contentful.createClient({
        // A valid access token within the Space
        accessToken: accessToken,
});



var limit = process.argv[process.argv.length-1]; limit = parseInt(limit) || 5000;


function getSlug(filename,ext){
    ext = ext === false ? false : true;
    return slug( path.basename( filename,path.extname(filename) ) ) + (ext ? path.extname(filename) : '')
}
function getUploadURL(file){
    return 'https://raw.githubusercontent.com/centre-for-effective-altruism/givingwhatwecan-org-static/master/src/images/uploads/'+file
}

var bodyImageRegex = new RegExp (/!\[(.)*?\]\(\/images\/uploads\/(.)+?\)/g);



client.getSpace(spaceID)
.then(function(space){



    // utility to get around 100-asset limit
    function getAllAssets(){
        return space.getAssets({limit:5000})
        .then(function(assets){
            if(assets.total > assets.limit){
                var iterations = Math.ceil(assets.total/assets.limit);
                var requests = [];
                for (var i = 0; i < iterations; i++) {
                    requests.push(space.getAssets({ skip: ( assets.limit * i) }));
                }
                return Promise.all(requests)
                .then(function(responses){
                    var allAssets = [];
                    responses.forEach(function(response){
                        allAssets = allAssets.concat(response);
                    })
                    return allAssets
                })
            } else {
                return assets;
            }
        })
        .then(function(assets){
            assets.sort(function(a,b){
                if(a.fields.file.en.fileName < b.fields.file.en.fileName){
                    return -1
                } else if(a.fields.file.en.fileName > b.fields.file.en.fileName){
                    return 1
                }
                return 0;
            })
            return assets;
        })
        .then(function(assets){
            assetNames = assets.map(function(asset){
                return asset.fields.file.en.fileName;
            })
            if(unique(assetNames).length < assetNames.length){
                console.log('Got',unique(assetNames).length,'/',assetNames.length,'unique asset names. Trying again...');
                return getAllAssets()
            } else {
                return assets
            }
        })
    }



    function uploadContentType(contentType){
        // pages pipeline
        var p = new Metalsmith(__dirname);
        p
        .use(function (files,metalsmith,done){
            var contentTypes = {};
            var authors = null;
            // get instance of space
            space.getContentTypes()
            .then(function(ct){
                ct.forEach(function(contentType){
                    contentTypes[contentType.name] = contentType.sys.id;
                })
            })
            .then(function(){
                // get all authors if we need them
                if(contentType === 'Post'){
                    authors = {};
                    return space.getEntries({'content_type':contentTypes['Author'],limit:5000})
                    .then(function(authorList){
                        authorList.forEach(function(author){
                            authors[author.fields.slug.en] = author.sys.id;
                        })
                    })
                }
            })
            .then(function(){
                // remove existing entries in the space
                return purgeContentType(contentType)
            })
            .then(function(){
                return getAllAssets()
                .then(function(assets){
                    console.log('Got',assets.length,'assets');
                    return {
                        assets: assets,
                        mapping: assets.map(function(asset){
                            return asset.fields.file.en.fileName;
                        })
                    }
                })
            })
            .then(function(mappedAssets){
                // process pages
                sortedFiles = minimatch.match(Object.keys(files),'content/'+contentType.toLowerCase()+'s/**/*');
                sortedFiles.sort();
                sortedFiles.sort(function(a,b){
                    // test for number of slashes in path to enable sorting by path depth
                    var numslashes = function(file){
                        return (path.parse(file).dir.match(/\//g)||[]).length
                    }
                    return numslashes(a) > numslashes(b) ? 1 : -1;
                });
                // create a queue for our requests
                var queue = new PromiseQueue({
                    concurrency: 1, // only do one at a time, so that we're guaranteed that parents are created before children
                });
                var sortedFiles = sortedFiles.slice(0,limit);
                var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:sortedFiles.length, width: 30 })
                var errors = [];
                sortedFiles.forEach(function(file){
                    queue.add(
                        function(){
                            var f = files[file];
                            var parent = null;
                            var title = null;
                            var name = null;
                            var ogImage = null;
                            var image = null;
                            var body;
                            return new Promise(function(resolve,reject){
                                // setup OG image
                                if(f.ogImage){
                                    var ogImageIndex = mappedAssets.mapping.indexOf(getSlug(f.ogImage));
                                    if(ogImageIndex > -1){
                                        ogImage = mappedAssets.assets[ogImageIndex].sys.id;
                                    } else { 
                                        console.log('Warning: did not find the OG image',getSlug(f.ogImage),'in the list of assets...')
                                    }
                                }
                                resolve();    
                            })
                            .then(function(){
                                if(f.image){
                                    var imageIndex = mappedAssets.mapping.indexOf(getSlug(f.image));
                                    if(imageIndex > -1){
                                        image = mappedAssets.assets[imageIndex].sys.id;
                                    }
                                }
                            })
                            .then(function(){
                                // setup body text
                                body = f.contents.toString();
                                // find any images in the body text
                                var images = body.match(bodyImageRegex) || [];
                                if(images.length > 0){
                                    images.forEach(function(imageMarkdown){
                                        var image = imageMarkdown.match(/\/images\/uploads\/[^\)]*/)[0];
                                        var imageIndex = mappedAssets.mapping.indexOf(getSlug(image));
                                        if(imageIndex > -1){
                                            replaceMarkdown = imageMarkdown.replace(image,mappedAssets.assets[imageIndex].fields.file.en.url);
                                            body = body.replace(imageMarkdown,replaceMarkdown);
                                        } else {
                                            console.log('Warning: did not find ',getSlug(image),' in the list of assets...')
                                        }
                                    })
                                }
                            })
                            .then(function(){
                                // look for a parent
                                if(f.parent){
                                    return space.getEntries({
                                      'content_type': contentTypes[contentType],
                                      'fields.slug': f.parent
                                    })
                                    .then(function(entries){
                                        // if there's one match, use it
                                        if(entries.length === 1){
                                            return entries[0];
                                        } else if (entries.length === 0) {
                                            // if there's no match, look for our parent in redirects instead
                                            return space.getEntries({
                                              'content_type': contentTypes[contentType],
                                              'fields.redirects': '/'+f.parent
                                            }).then(function(entries){
                                                // if there's one match, use it
                                                if(entries.length === 1){
                                                    return entries[0];
                                                } else {
                                                    return null;
                                                }

                                            })
                                        }
                                    }).then(function(p){
                                        if(p){
                                            parent = p;
                                        } else {
                                            console.error('Warning: could not find a parent for file',file,'- looking for',f.parent);
                                        }
                                    })
                                }
                            })
                            .then(function(){

                                // build fields object
                                var fields = {}

                                // title or name
                                if(f.title){
                                    fields.title = {'en':f.title};
                                }
                                if(f.name){
                                    fields.name = {'en':f.name};
                                }

                                // short title
                                if(f.menuTitle){
                                    fields.shortTitle =  {'en': f.menuTitle}
                                }
                                // body
                                if(contentType === 'Author'){
                                    fields.bio = {'en': body}
                                } else {
                                    fields.body = {'en': body.substr(0,49999)}
                                }
                                // navigation
                                if(f.navigation){
                                    navigation = {'en': f.navigation }
                                }
                                // slug
                                if(f.slug){
                                    fields.slug = {'en': slug(path.basename(f.slug)) }
                                } else if (contentType==='Author'){
                                    fields.slug = {'en': slug(f.name) }
                                }

                                // image
                                if(image && contentType==='Author'){
                                    fields.photo = {'en' : { sys: { type: 'Link', linkType: 'Asset', id: image } } }
                                }
                                // author
                                if(f.author){
                                    f.author = Array.isArray(f.author) ? f.author : [f.author];
                                    fields.author = {'en' : [] }
                                    f.author.forEach(function(author){
                                        if(authors.hasOwnProperty(author)){
                                            fields.author.en.push({ sys: { type: 'Link', linkType: 'Entry', id: authors[author] } } )
                                        } else {
                                            console.log('Warning, did not find an author for',f.title || f.name)
                                        }
                                    })
                                }
                                // creation date
                                if(contentType === 'Post'){
                                    fields.date = {'en' : moment(f.date + ' ' + f.time,'YYYY-MM-DD h:ma').toISOString() };
                                }
                                // setup redirects
                                if(f.redirects){
                                    var redirects = [];
                                    f.redirects.forEach(function(redirect){
                                        if(redirect.substr(0,1)!=='/');
                                        redirect = '/' + redirect;
                                        redirects.push(redirect);
                                    })
                                    if(redirects.length>0){
                                        fields.redirects = { 'en': redirects };
                                    }
                                }
                               

                                // add OG Image
                                if(ogImage){
                                    fields.ogImage = {'en' : { sys: { type: 'Link', linkType: 'Asset', id: ogImage } } }
                                }
                                // add parent
                                if(parent){
                                    fields.parent = {'en' : { sys: { type: 'Link', linkType: 'Entry', id: parent.sys.id } } }
                                }

                                // actually create the entry!
                                return space.createEntry(contentTypes[contentType], {
                                    fields: fields
                                })
                                .then(function(entry){
                                    var title  = entry.fields.title || entry.fields.name; title = title.en;
                                    bar.tick({asset:'Created entry ' + entry.sys.id + ': '+title.substr(0,20)})
                                })
                                .catch(function(error){
                                    bar.tick({asset:'Error creating new '+contentType})
                                    errors.push({file:file,error:error});
                                })
                            })
                            .catch(function(err){
                                console.error(err);
                            })
                        }
                    )
                });
                console.log('Uploading',queue._queue.length,contentType+'s...');
                return queue.start().then(function(results){
                    console.log(contentType+'s finished uploading!');
                    if(errors.length>0){
                        errors.forEach(function(error){
                            console.log('---');
                            console.error(error.file);
                            console.error(error.error);
                        })
                    }
                });
            })

        }).build(function(err,files){
            if(err) console.error(err);
        })
        ;
    }

    //
    function publishContentType(contentType){
        space.getContentTypes()
        .then(function(ct){
            contentTypes = {};
            ct.forEach(function(contentType){
                contentTypes[contentType.name] = contentType.sys.id;
            })
            return space.getEntries({'content_type':contentTypes[contentType],limit:5000})
        })
        .then(function(entries){
            // create a queue for our requests
            var queue = new PromiseQueue({
                concurrency: 10, // optional, how many items to process at a time 
                delay: 500
            });
            var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:entries.length, width: 30 })
            var errors = [];
            entries.forEach(function(entry){
                var title = entry.fields.title || entry.fields.name; title = title.en;
                if(!entry.sys.hasOwnProperty('publishedVersion')){
                    queue.add(function(){
                        return space.publishEntry(entry)
                        .then(function(){
                            bar.tick({asset:'Published ' + title })
                        })
                        .catch(function(error){
                            bar.tick({asset:'Could not publish ' + title })
                            errors.push({page:entry.fields.title.en, errors:JSON.parse(error.message.split('Validation error ')[1]).details.errors})
                        })
                    })
                } else {
                    bar.tick({asset:title + ' already published' })
                }
            })
            if(queue._queue.length>0){
                return queue.start()
                .then(function(){
                    if(errors.length>0){
                        console.error('Most ',contentType+'s were published, but the following pages have validation errors');
                        errors.forEach(function(error){
                            console.log('-----');
                            console.log(error.page + ':');
                            error.errors.forEach(function(error){
                                console.log('Field',error.path[1])
                                console.log('Value:',error.value)
                                console.log('Details:',error.details)
                            })
                        })

                    } else {
                        console.log('All',contentType+'s are published!')
                    }
                })
            } else {
                console.log('All',contentType+'s already published!');
            }
        })
    }

    function purgeContentType(contentType){
        return space.getContentTypes()
        .then(function(ct){
            contentTypes = {};
            ct.forEach(function(contentType){
                contentTypes[contentType.name] = contentType.sys.id;
            })
            return space.getEntries({'content_type':contentTypes[contentType],limit:5000})
        })
        .then(function(entries){
            // unpublish all the entries
            if(entries && entries.length>0) {
                console.log('Deleting existing',contentType+'s...');

                // create a queue for our requests
                var queue = new PromiseQueue({
                    concurrency: 10, // optional, how many items to process at a time,
                    delay: 500
                });

                var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:entries.length, width: 30 })
                entries.forEach(function(entry){
                    var title = entry.fields.title || entry.fields.name;
                    title = title.en
                    if(typeof entry.sys.publishedVersion !== 'undefined'){
                         queue.add(
                            function(){
                                return space.unpublishEntry(entry.sys.id)
                                .then(function(){
                                    return space.deleteEntry(entry.sys.id)
                                })
                                .then(function(){
                                    bar.tick({asset:'Unpublished/deleted '+title})
                                })
                            }
                        )
                    } else {
                        queue.add(
                            function(){
                                return space.deleteEntry(entry.sys.id)
                                .then(function(){
                                    bar.tick({asset:'Deleted '+title})
                                })
                            }
                        )
                    }
                });
                return queue.start().then(function(results){
                    console.log('Finished deleting',contentType+'s...');
                })
            } else {
                console.log('No',contentType+'s to delete')
            }
        })
    }

    // assets pipeline
    function uploadAssets() {
        console.log('Uploading assets...')
        var assets;
        missingAssets()
        .then(function(missing){
            console.log(missing);
            console.log('There are',missing.length,'assets missing from the server');
            // create a queue for our requests
            var queue = new PromiseQueue({
                concurrency: 5, // optional, how many items to process at a time 
                delay: 1000
            });
            // add images to the queue
            missing = missing.slice(0,limit);
            // progress bar
            var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:missing.length, width: 30 })
            missing.forEach(function(file){
                queue.add(function(){
                    return space.createAsset({
                        fields: {
                            title: {'en': getSlug(file,false) },
                            file : {
                                en: {
                                    fileName: getSlug(file),
                                    contentType: mime.lookup(file),
                                    upload: getUploadURL(file)
                                }
                            }
                        }
                    })
                    .then(function(asset){
                        bar.tick({asset: 'Created asset ' + asset.sys.id + ' for ' + file})
                    })
                })
            })

            console.log('Starting upload...')
            queue.start().then(function(){
                console.log('Finished uploading images!')
            })
        })

    }

    function processAssets() {
        var assets;
        getAllAssets()
        .then(function(a){
            assets = a || [];
        })
        .then(function(){
            var queue = new PromiseQueue({
                concurrency: 5, // optional, how many items to process at a time,
                delay: 1000 
            });
            var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:100, width: 30 })
            bar.total = 0;  
            assets.forEach(function(asset){
                if(asset.sys && asset.fields.file.en.hasOwnProperty('upload')){
                    bar.total++;
                    queue.add(function(){
                        return space.processAssetFile(asset,'en')
                        .then(function(){
                            bar.tick({asset:'Processed '+asset.fields.file.en.fileName+' upload url'+asset.fields.file.en.upload});
                        })
                        .catch(function(error){
                            console.log('Error:')
                            console.log(error.message)
                        })
                    });
                } else {
                    console.log('Cant process',asset.fields.file.en.fileName);
                }
            })
            console.log('Processing',queue._queue.length,'assets...');
            return queue.start().then(function(results){
                console.log('Finished processing...');
            });
        })
    }

    function publishAssets() {
        var assets;
        getAllAssets()
        .then(function(a){
            assets = a || [];
        })
        .then(function(){
            var queue = new PromiseQueue({
                concurrency: 10, // optional, how many items to process at a time,
                delay: 1000 
            });        
            var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:100, width: 30 })
            bar.total = 0;
            assets.forEach(function(asset){
                if(asset.sys && asset.fields.file.en.hasOwnProperty('url')){
                    bar.total++;
                    queue.add(function(){
                        return space.publishAsset(asset)
                        .then(function(){
                            bar.tick({asset:'Published '+asset.fields.file.en.fileName});
                        })
                        .catch(function(error){
                            console.log('Error:')
                            console.log(error.message)
                        })
                    });
                }
            })
            console.log('Publishing',queue._queue.length,'assets...');
            return queue.start().then(function(results){
                console.log('Finished publishing...');
            });
        })
    }

    // purge assets
    function purgeAssets(){
        var assets;
        getAllAssets()
        .then(function(a){
            assets = a || [];
        })
        .then(function(){
            var queue = new PromiseQueue({
                concurrency: 5, // optional, how many items to process at a time,
                delay: 1000 
            });
            assets.forEach(function(asset){
                if(asset.sys && asset.sys.publishedVersion){
                    queue.add( function(){
                        console.log('Unpublishing',asset.fields.file.en.fileName);
                        return space.unpublishAsset(asset)
                    } );
                }
            })
            if(queue._queue.length>0){
                console.log('Unpublishing',queue._queue.length,'assets...');
                return queue.start().then(function(results){
                    console.log('Finished unpublising...');
                });
            }
        })
        .then(function(){
            // create progress bar
            var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:100, width: 30 })
            bar.total = 0;
            // delete assets

             // create a queue for our requests
            var queue = new PromiseQueue({
                concurrency: 5, // optional, how many items to process at a time,
                interval: 1000
            });
            assets.forEach(function(asset){
                if(asset.sys && !asset.sys.publishedVersion){
                    bar.total++;
                    queue.add( function(){
                        return space.deleteAsset(asset)
                        .then(function(){
                            bar.tick({asset:asset.fields.title.en});
                        })
                        .catch(function(error){
                            console.log('Error deleting',asset.sys.id)
                            console.log(error.message)
                        });
                    } );
                }
            })
            if(queue._queue.length>0){
                console.log('Starting',queue._queue.length,'deletions...');
                return queue.start().then(function(results){
                    console.log('Finished deleting...');
                });
            } else {
                    console.log('No assets to delete!');
            }
        })
    }

    function dedupeAssets(){
        var assets;
        var dupes = [];
        getAllAssets()
        .then(function(a){
            assets = a || [];
            var filenames = [];
            console.log('Total assets',assets.length)
            assets.forEach(function(asset){
                var filename = asset.fields.file.en.fileName
                if( filenames.indexOf(filename) === -1 ){
                    // console.log('Only one instance of',filename,'so far')
                    filenames.push(filename);
                } else {
                    // console.log('Duplicate detected for',filename)
                    dupes.push(asset)
                }
            })
            console.log('Singletons',filenames.length)
            console.log('Duplicates',dupes.length)

        })
        .then(function(){
            // create a queue for our requests
            var queue = new PromiseQueue({
                concurrency: 5, // optional, how many items to process at a time,
                interval: 1000
            });
            dupes.forEach(function(asset){
                if(asset.sys && asset.sys.publishedVersion){
                    queue.add(function(){
                        console.log('Unpublishing',asset.fields.file.en.fileName);
                        return space.unpublishAsset(asset)
                    });
                }
            })
            if(queue._queue.length>0){
                console.log('Unpublishing',queue._queue.length,'assets...');
                return queue.start().then(function(results){
                    console.log('Finished unpublishing...');
                });
            }
        })
        .then(function(){
            // create progress bar
            var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:100, width: 30 })
            bar.total = 0;
            // delete assets

            // create a queue for our requests
            var queue = new PromiseQueue({
                concurrency: 5, // optional, how many items to process at a time,
                interval: 1000
            });
            dupes.forEach(function(asset){
                bar.total++;
                queue.add(function(){
                    return space.deleteAsset(asset)
                    .then(function(){
                        bar.tick();
                    })
                    .catch(function(error){
                        console.log('Error deleting',asset.sys.id)
                        console.log(error.message)
                    });
                });
            })
            if(queue._queue.length>0){
                console.log('Deleting',queue._queue.length,'assets...');
                return queue.start().then(function(results){
                    console.log('Finished deleting...');
                });
            } else {
                console.log('Nothing to delete...');
            }
        })
    }


    function missingAssets(){
        var assetFileNames;
        var assetFiles;

        // get all files in local directory
        return new Promise(function(resolve,reject){
            var imageDir = './src/images/uploads'
            fs.readdir(imageDir, function(err,files){
                if (err) throw err;
                assetFiles = files.map(function(file){
                    return getSlug(file);
                }).filter(function (file) {
                    return minimatch(file,'*.+(png|jpg|gif)');
                });
                resolve();
            })
        })
        .then(function(){
            // get all names from remote directory
            return getAllAssets()
            .then(function(a){
                assetFileNames = a.map(function(asset){
                    return asset.fields.file.en.fileName;
                })
                console.log('Checking',assetFileNames.length,'filenames')
                if(assetFileNames.length !== unique(assetFileNames).length){
                    console.log('Warning — duplicate assets exist')
                    console.log('Asset file names:',assetFileNames.length)
                    console.log('Unique asset file names:',unique(assetFileNames).length)

                }
            })
        })
        .then(function(){
            var missing = [];
            assetFiles.forEach(function(assetFile){
                if(assetFileNames.indexOf(getSlug(assetFile)) === -1){
                    missing.push(assetFile)
                }
            })
            return missing;
        })
        .then(function(possiblyMissing){
            // double check that these images are actually missing...
            var queue = new PromiseQueue({
                concurrency:10, delay: 400
            })
            var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:possiblyMissing.length, width: 30 })
            var missing = [], oddballs = [];
            possiblyMissing.forEach(function(file){
                queue.add(function(){
                    return space.getAssets({'fields.file.en.fileName':file})
                    .then(function(assets){
                        bar.tick({asset: file})
                        if(assets.length===0){
                            missing.push(file);
                        } else {
                            oddballs.push(file);
                        }
                    })
                })
            })
            console.log('Looking for',queue._queue.length,'missing images...')
            return queue.start()
            .then(function(results){
                if(oddballs.length>0){
                    console.log('Removed',oddballs.length,'files erroneously picked up on the first pass');
                }
                return missing;
            })
        });
    }

    /*function missingAssets(){
        var assets;
        return new Promise(function(resolve,reject){
            var imageDir = './src/images/uploads'
            fs.readdir(imageDir, function(err,files){
                if (err) throw err;
                resolve(files.map(function(file){
                    return getSlug(file);
                }).filter(function (file) {
                    return minimatch(file,'*.+(png|jpg|gif)');
                }));
            })
        })
        .then(function(assetFiles){
            // queue
            var queue = new PromiseQueue({
                concurrency:10, delay: 400
            })
            var bar = new ProgressBar('[:bar] :current/:total :percent :eta :asset', { total:assetFiles.length, width: 30 })
            var missing = [];
            assetFiles.forEach(function(file){
                queue.add(function(){
                    return space.getAssets({'fields.file.en.fileName':file})
                    .then(function(assets){
                        bar.tick({asset: file})
                        if(assets.length===0){
                            missing.push(file);
                        }
                    })
                })
            })
            console.log('Looking for missing images...')
            return queue.start()
            .then(function(results){
                return missing;
            })
        });
    }*/



    if(process.argv[2] === 'upload') {
        uploadContentType(process.argv[3]);
    }
    else if(process.argv[2] === 'publish') {
        publishContentType(process.argv[3]);
    }
    else if(process.argv[2] === 'purge') {
        purgeContentType(process.argv[3]);
    }  
    else if(process.argv[2] === 'assets') {
        uploadAssets();
    }
    else if(process.argv[2] === 'processassets') {
        processAssets();
    }
    else if(process.argv[2] === 'publishassets') {
        publishAssets();
    } 
    else if(process.argv[2] === 'purgeassets') {
        purgeAssets();
    }
    else if(process.argv[2] === 'dedupeassets') {
        dedupeAssets();
    }
    else if(process.argv[2] === 'missingassets') {
        missingAssets();
    }
    else {
        console.error('Error, missing required argument. Use `pages` or `assets`')
    }

})
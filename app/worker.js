const redis = require('redis');
const Queue = require('bee-queue');
const GCPUtility = require('./gcp-utility.js');

const PROJECT_ID = process.env.PROJECT_ID;
const KEY_FILE = process.env.GCP_KEY_FILE_FULL_PATH;

console.log('create queue for worker');
const eq = new Queue('watchEvents', {
    redis: {
        host: 'redis'
    }
});

const rclient = redis.createClient( { host: 'redis' });

eq.on('ready', () => {
    console.log('Worker ready');
    
    eq.process((job, done) => {
        const evt = job.data.event;
        console.log('Handling', evt);
        
        if (evt === 'addDir' || evt === 'removeDir') {
            done( null, 'Nothing to do.');
        } else {
            const bucketName = job.data.targetBucket;
            const gcpUtility = new GCPUtility( PROJECT_ID, KEY_FILE, bucketName );
            if (evt === 'inventory') {
                gcpUtility.inventory(job.data.projection).then( ( result ) => {
                    const cacheKey = [bucketName,'inventory'].join('::');
                    console.log('Cache inventory; key =', cacheKey);
                    rclient.set(cacheKey, JSON.stringify( result ), ( err, reply ) => {
                        if ( err ) {
                            console.log('ERROR', err);
                        } else {
                            console.log( reply );
                            done( null, cacheKey );
                        }
                    });
                    
                }).catch( err => done( err ));
            } else {
                const sourcePath = job.data.sourceFileName;
                const targetPath = job.data.targetFileName;
                if (evt === 'update') {
                    gcpUtility.upload( sourcePath, targetPath ).then( ( result ) => {
                        done( null, result );
                    }).catch( ( err ) => done( err ));
                } else
                if (evt === 'remove') {
                    gcpUtility.remove( targetPath ).then( ( result ) => {
                        done( null, result );
                    }).catch( err => done( err ));
                }
            }
        }
    });
});

eq.on('error', (err) => {
    console.log('Queue error: ', err.message);
});
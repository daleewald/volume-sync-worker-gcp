const redis = require('redis');
const Queue = require('bee-queue');
const logger = require('./logger');
const GCPUtility = require('./gcp-utility.js');

const PROJECT_ID = process.env.PROJECT_ID;
const KEY_FILE = process.env.GCP_KEY_FILE_FULL_PATH;

let queueName = 'GCP-INVENTORY-EVENTS';

logger.info('Open Queue', queueName);
const eq = new Queue(queueName, {
    redis: {
        host: 'redis'
    }
});

const rclient = redis.createClient( { host: 'redis' });

eq.on('ready', () => {
    logger.info('Worker ready');
    
    eq.process((job, done) => {
        const evt = job.data.event;
        logger.debug('Handling', evt);
        
        if (evt === 'addDir' || evt === 'removeDir') {
            done( null, 'Nothing to do.');
        } else {
            const bucketName = job.data.targetBucket;
            const gcpUtility = new GCPUtility( PROJECT_ID, KEY_FILE, bucketName );
            if (evt === 'inventory') {
                gcpUtility.inventory(job.data.projection).then( ( result ) => {
                    const cacheKey = [queueName, bucketName,'inventory'].join(':');
                    logger.debug('Cache inventory; key =', cacheKey);
                    rclient.set(cacheKey, JSON.stringify( result ), ( err, reply ) => {
                        if ( err ) {
                            logger.error('ERROR', err);
                            done( err );
                        } else {
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
    logger.error(queueName, 'error: ', err.message);
});
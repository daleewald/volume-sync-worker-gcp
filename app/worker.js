const {Storage} = require('@google-cloud/storage');
const Queue = require('bee-queue');

const PROJECT_ID = process.env.PROJECT_ID;
const KEY_FILE = '/keyfile';
const BUCKET_NAME = process.env.BUCKET_NAME;

const storage = new Storage({
    projectId: PROJECT_ID,
    keyFilename: KEY_FILE
});
const bucket = storage.bucket( BUCKET_NAME );

console.log('create queue for worker');
const eq = new Queue('watchEvents', {
    redis: {
        host: 'redis'
    }
});

eq.on('ready', () => {
    console.log('Worker ready');
    
    eq.process((job, done) => {
        const evt = job.data.event;
        const sourcePath = job.data.sourceFileName;
        const targetPath = job.data.targetFileName;

        console.log('Processing ', evt, targetPath);

        if (evt === 'addDir' || evt === 'removeDir') {
            done( null, 'Nothing to do.');
        } else 
        if (evt === 'update') {
            console.log('Beginning upload: ', targetPath);
            bucket.upload( sourcePath, { destination: targetPath } ).then( ( file ) => {
                console.log('Upload completed, generation:', file[0].metadata.generation);
                done(null, {result: 'Uploaded', file: targetPath, generation: file[0].metadata.generation});
            }).catch( ( err ) => {
                done( err );
            });
        } else
        if (evt === 'remove') {
            const file = bucket.file( targetPath );
            file.exists().then( ( exists ) => {
                if ( exists[0] ) {
                    file.delete((err, resp) => {
                        if (err) {
                            console.log( resp );
                            done( err );
                        } else {
                            console.log('Remove completed for', targetPath);
                            done(null, {result: 'Deleted', file: targetPath});
                        }
                    });
                } else {
                    done(null, {result: 'Did not exist to remove', file: targetPath});
                }
            });
        }
    });
});

eq.on('error', (err) => {
    console.log('Queue error: ', err.message);
});
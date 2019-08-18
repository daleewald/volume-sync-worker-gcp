const {Storage} = require('@google-cloud/storage');

class GCPUtility {
    storage;
    bucket;
    fileProjection = {
        name: 'name',
        updated: ['metadata','updated']
    }

    constructor( projectId, keyFile, bucketName ) {
        this.setup(projectId, keyFile, bucketName);
    }

    setup( PROJECT_ID, KEY_FILE, BUCKET_NAME ) {
        this.storage = new Storage({
            projectId: PROJECT_ID,
            keyFilename: KEY_FILE
        });
        this.bucket = this.storage.bucket( BUCKET_NAME );
    }

    async upload( sourcePath, targetPath ) {
        console.log('Beginning upload:', targetPath);
        return this.bucket.upload( sourcePath, { destination: targetPath } ).then( ( file ) => {
            console.log('Upload complete:', targetPath);
            return {result: 'Uploaded', file: targetPath, generation: file[0].metadata.generation};
        });
    }

    async remove( targetPath ) {
        const file = this.bucket.file( targetPath );
        return file.exists().then( ( exists ) => {
            let result;
            if ( exists[0] ) {
                file.delete((err, resp) => {
                    if (err) {
                        throw err;
                    } else {
                        result = {result: 'Deleted', file: targetPath};
                    }
                });
            } else {
                result = {result: 'Did not exist to remove', file: targetPath};
            }
            return result;
        });
    }

    async inventory(projection) {
        return this.bucket.getFiles().then( result => {
            return result[0].map( file => {
                let p = {};
                projection.forEach( key => {
                    if (Array.isArray(this.fileProjection[key])) {
                        let t = file;
                        this.fileProjection[key].forEach( (v) => {
                            t = t[v];
                        });
                        p[key] = t;
                    } else {
                        p[key] = file[this.fileProjection[key]];
                    }
                });
                return p;
            });
        });
    }
}

module.exports = GCPUtility;
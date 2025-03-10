import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Variables } from '@fermyon/spin-sdk';
import { AutoRouter } from 'itty-router';

let router = AutoRouter();

router
    .get("/", () => new Response("hello universe"))
    .get('/hello/:name', ({ name }) => `Hello, ${name}!`)

    .post('api/v1/upload/:bucket/:file', async (req) => {
        let { bucket, file } = req.params;

        let region = Variables.get('s3_region');
        let endpoint = Variables.get('s3_endpoint');
        let accessKeyId = Variables.get('s3_access_key_id');
        let secretAccessKey = Variables.get('s3_secret_access_key');
        if (!region || !endpoint || !accessKeyId || !secretAccessKey) {
            return new Response('Missing environment variables for accessing S3', { status: 500 });
        }

        let s3 = new S3Client({
            region,
            endpoint,
            credentials: {
                accessKeyId,
                secretAccessKey
            }
        });

        try {
            const body = await req.arrayBuffer();

            const command = new PutObjectCommand({
                Bucket: bucket,
                Key: file,
                Body: new Uint8Array(body),
            });

            await s3.send(command);

            return new Response('File uploaded successfully', { status: 200 });
        } catch (error) {
            console.error('Error uploading file:', error);
            return new Response('Error uploading file to S3', { status: 500 });
        }
    })

//@ts-ignore
addEventListener('fetch', (event: FetchEvent) => {
    event.respondWith(router.fetch(event.request));
});

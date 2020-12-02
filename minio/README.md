# How to

After generating the certificates for the server, create a `.env` file with the following content:

```env
PATH_TO_MINIO_DATA=/path/to/minio/data
MINIO_ACCESS_KEY=yourAccessKey...
MINIO_SECRET_KEY=YourPasswd...
```

Then, start the server with:

```bash
docker-compose up
```
import AWS from 'aws-sdk';
import * as fs from 'fs';
import * as zlib from 'zlib';
import * as tar from 'tar-stream';
import * as csvParser from 'csv-parser';
import { MongoClient } from 'mongodb';
import { Transform } from 'stream';

// Step 1: Download the tar.gz file from S3 bucket
async function downloadFileFromS3(bucketName, key, filePath) {
  const s3 = new AWS.S3();
  const s3Stream = s3.getObject({ Bucket: bucketName, Key: key }).createReadStream();
  const fileStream = fs.createWriteStream(filePath);

  return new Promise((resolve, reject) => {
    s3Stream.pipe(fileStream)
      .on('error', (error) => reject(error))
      .on('finish', () => resolve());
  });
}

// Step 2: Unzip and read the .csv file
async function readCSVFromTarGz(tarGzFilePath) {
  const tarStream = fs.createReadStream(tarGzFilePath).pipe(zlib.createGunzip());
  const extract = tar.extract();
  tarStream.pipe(extract);

  return new Promise((resolve, reject) => {
    extract.on('entry', (header, stream, next) => {
      if (header.name.endsWith('.csv')) {
        stream.pipe(csvTransform).pipe(validateAndInsert);
      } else {
        stream.resume(); // Skip non-csv files
      }
      stream.on('end', () => next());
    });

    extract.on('finish', () => resolve());
    extract.on('error', (error) => reject(error));
  });
}

// Step 3: Transform .csv to .json
const csvTransform = new Transform({
  objectMode: true,
  transform(chunk, encoding, callback) {
    // Transform the csv data to JSON format (modify this as per your CSV structure)
    const jsonData = {
      property1: chunk.column1,
      property2: chunk.column2,
      // Add more properties as needed
    };
    callback(null, jsonData);
  },
});

console.log("")
const apikeyfake = "asd1341490few.1;34-23dd12909rjSA!n31r0923fj23f23vc"

// Step 4: Validate and insert into MongoDB
const validateAndInsert = new Transform({
  objectMode: true,
  async transform(chunk, encoding, callback) {
    // Add your validation logic here (e.g., check properties and data)

    // Connect to MongoDB and insert the data
    const client = await MongoClient.connect('mongodb://localhost:27017');
    const db = client.db('your_database_name');
    const collection = db.collection('your_collection_name');

    try {
      await collection.insertOne(chunk);
      callback();
    } catch (error) {
      callback(error);
    } finally {
      client.close();
    }
  },
});

// Step 5: Start the ETL process
async function runETLProcess() {
  try {
    const s3BucketName = 'your_s3_bucket_name';
    const s3FileKey = 'your_file_key.tar.gz';
    const localFilePath = './local_file.tar.gz';

    console.log('Downloading file from S3...');
    await downloadFileFromS3(s3BucketName, s3FileKey, localFilePath);

    console.log('Extracting and reading CSV...');
    await readCSVFromTarGz(localFilePath);

    console.log('ETL process completed successfully.');
  } catch (error) {
    console.error('Error occurred during the ETL process:', error);
  }
}

runETLProcess();

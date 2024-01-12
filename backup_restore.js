const { MongoClient, ObjectId } = require('mongodb');
const fs = require('fs-extra');

// MongoDB connection URLs
const sourceUrl = 'mongodb+srv://sivaklu92:Amphe@cluster0.rffejl0.mongodb.net/test';
const destinationUrl = 'mongodb+srv://clanizonlog:' + encodeURIComponent('clanizon@123') + '@cluster0.r9psi2c.mongodb.net/'

// database name you want to backup
const dbNameToBackup = 'pms';

const connectToMongoDB = async (url) => {
  const client = new MongoClient(url, { useNewUrlParser: true, useUnifiedTopology: true });
  await client.connect();
  return client;
};

// Backup_function
const backupAllCollectionsInDatabase = async (dbName) => {
  try {
    const sourceClient = await connectToMongoDB(sourceUrl);
    const collections = await sourceClient.db(dbName).listCollections().toArray();

    for (const collection of collections) {
      await fs.ensureDir(`./backupdata/${dbName}`);
      const data = await sourceClient.db(dbName).collection(collection.name).find().toArray();
      await fs.writeJson(`./backupdata/${dbName}/${collection.name}.json`, data);
      console.log(`Backup successful for ${dbName}.${collection.name}`);
    }
    console.log(`Backup completed for database ${dbName}`);
  } catch (error) {
    console.error(`Backup failed for ${dbName}:`, error);
  }
};

// Restore function
const restoreAllCollectionsInDatabase = async (dbName) => {
  try {
    // Check if the backupdata directory exists
    if (!(await fs.pathExists(`./backupdata/${dbName}`))) {
      console.log(`No backup files found for ${dbName}. Exiting restore process.`);
      return;
    }

    const destinationClient = await connectToMongoDB(destinationUrl);

    // Read the backup files then restore to each collection
    const collections = await fs.readdir(`./backupdata/${dbName}`);
    for (const collection of collections) {
      const data = await fs.readJson(`./backupdata/${dbName}/${collection}`);

      for (const document of data) {
        try {
          await destinationClient
            .db(dbName)
            .collection(collection.split('.json')[0])
            .updateOne({ _id: document._id }, { $set: document }, { upsert: true });
          console.log(`Restore successful for ${dbName}.${collection}`);
        } catch (error) {
          if (error.code === 11000) {
            console.log(`Duplicate key error: Skipping document with _id ${document._id}`);
          } else {
            console.error(`Error restoring document for ${dbName}.${collection}:`, error);
          }
        }
      }
    }
    console.log(`Restore completed for database ${dbName}`);
  } catch (error) {
    console.error(`Restore failed for ${dbName}:`, error);
  }
};

// Connect to MongoDB and perform backup and restore
const runscript = async () => {
  try {
    await backupAllCollectionsInDatabase(dbNameToBackup);
    await restoreAllCollectionsInDatabase(dbNameToBackup);

    // Clean the backupdata 
    await fs.remove(`./backupdata/${dbNameToBackup}`);

    // Close MongoDB connections
    process.exit();
  } catch (error) {
    console.error('Script failed:', error);
    process.exit(1);
  }
};

runscript();

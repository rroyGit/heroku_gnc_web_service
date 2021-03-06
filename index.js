#!/usr/bin/env node

'use strict';

const {promisify} = require('util'); // tranform regular functions to return promises
const readFile = promisify(require('fs').readFile); // make readFile() async
const writeFile = promisify(require('fs').writeFile); // make writeFile() async
const Path = require('path');

const pcServer = require("./pc_server");
const processor = require("./processor");
const PCDatabase = require('./pc_database');

function usage () {
    console.error("usage: %s PORT", Path.basename(process.argv[1]));
    process.exit(1);
}

function getPort(portArg) {
    let port = Number(portArg);
    if (!port) {
      console.error(`bad port '${portArg}'`);
      usage();
    }
    return port;
}

async function shutdown(event, resources) {
    if (Object.keys(resources).length > 0) {
      console.log(`\nBing Web Service is ... shutting down on ... ${event}`);

      if (resources.server) {
        await resources.server.close();
        delete resources.server;
      }

      if (resources.databaseConnection) {
        //don't clear heroku MongoDB database collections
        //await resources.databaseConnection.clear();
        await resources.databaseConnection.close();
        delete resources.databaseConnection;
      }
    }
    process.exit();
}

function cleanupResources(resources) {
    const events = [ 'SIGINT', 'SIGTERM', 'exit' ];
    for (const event of events) {
        process.on(event, async () => await shutdown(event, resources));
    }
}

const PID_FILE = 'Process_ID.txt';
async function go(args) {
    const resources = {};
    try {
      const port = process.env.PORT || getPort(args[0]);
      const dbUrl = process.env.MONGODB_URI || args[1];
      const file_processor = processor.init();

      const pcDatabase = new PCDatabase(dbUrl);
      resources.databaseConnection = pcDatabase;
      await pcDatabase.init();

      resources.server = pcServer.init(port, file_processor, pcDatabase);
      await writeFile(PID_FILE, `${process.pid}\n`);
    } catch (err) {
      console.error(err);
    } finally {
      cleanupResources(resources);
    }
  }


if (process.argv.length < 3) {
    usage();
} else {
    // argv[0] = /usr/local/bin/node
    // argv[1] = /mnt/c/Users/<username>/Desktop/Hyperloop_server/index.js
    go(process.argv.slice(2));
}

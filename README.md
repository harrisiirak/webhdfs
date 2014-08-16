node-webhdfs
============

[![Build Status](https://travis-ci.org/harrisiirak/webhdfs.png?branch=master)](https://travis-ci.org/harrisiirak/webhdfs)
[![NPM version](https://badge.fury.io/js/webhdfs.png)](http://badge.fury.io/js/webhdfs)


Hadoop WebHDFS REST API (2.2.0) client library for node.js with `fs` module like (asynchronous) interface.  

## Examples

Writing to the remote file:

```javascript
var WebHDFS = require('webhdfs');
var hdfs = WebHDFS.createClient();

var localFileStream = fs.createReadStream('/path/to/local/file');
var remoteFileStream = hdfs.createWriteStream('/path/to/remote/file');

localFileStream.pipe(remoteFileStream);

remoteFileStream.on('error', function onError (err) {
  // Do something with the error
});

remoteFileStream.on('finish', function onFinish () {
  // Upload is done
});
```

Reading from the remote file:

```javascript
var WebHDFS = require('webhdfs');
var hdfs = WebHDFS.createClient({
  user: 'webuser',
  host: 'localhost',
  port: 80,
  path: '/webhdfs/v1'
});

var remoteFileStream = hdfs.createReadStream('/path/to/remote/file');

remoteFileStream.on('error', function onError (err) {
  // Do something with the error
});

remoteFileStream.on('data', function onChunk (chunk) {
  // Do something with the data chunk
});

remoteFileStream.on('finish', function onFinish () {
  // Upload is done
});
```

## TODO

* Implement all calls (GETCONTENTSUMMARY, GETFILECHECKSUM, GETHOMEDIRECTORY, GETDELEGATIONTOKEN, RENEWDELEGATIONTOKEN, CANCELDELEGATIONTOKEN, SETREPLICATION, SETTIMES)
* Improve documentation
* More examples

# Tests

Running tests:

```bash
npm test
```

# License

MIT

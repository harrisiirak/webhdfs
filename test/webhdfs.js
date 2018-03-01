'use strict';

var fs = require('fs');
var demand = require('must');
var sinon = require('sinon');

var WebHDFS = require('../lib/webhdfs');
var WebHDFSProxy = require('webhdfs-proxy');
var WebHDFSProxyMemoryStorage = require('webhdfs-proxy-memory');

var user = require('os').userInfo().username;

describe('WebHDFS', function () {
  var path = '/files/' + Math.random();
  var hdfs = WebHDFS.createClient({
    user: user,
    port: 45000
  });

  this.timeout(10000);

  before(function (done) {
    var opts = {
      path: '/webhdfs/v1',
      http: {
        port: 45000
      }
    };

    WebHDFSProxy.createServer(opts, WebHDFSProxyMemoryStorage, done);
  });

  it('should make a directory', function (done) {
    hdfs.mkdir(path, function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should create and write data to a file', function (done) {
    hdfs.writeFile(path + '/file-1', 'random data', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should append content to an existing file with no user', function (done) {
    var hdfs = WebHDFS.createClient({
      port: 45000
    });
    hdfs.appendFile(path + '/file-1', 'more random data', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should create and stream data to a file', function (done) {
    var localFileStream = fs.createReadStream(__filename);
    var remoteFileStream = hdfs.createWriteStream(path + '/file-2');
    var spy = sinon.spy();

    localFileStream.pipe(remoteFileStream);
    remoteFileStream.on('error', spy);

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();
      done();
    });
  });

  it('should append stream content to an existing file', function (done) {
    var localFileStream = fs.createReadStream(__filename);
    var remoteFileStream = hdfs.createWriteStream(path + '/file-2', true);
    var spy = sinon.spy();

    localFileStream.pipe(remoteFileStream);
    remoteFileStream.on('error', spy);

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();

      done();
    });
  });

  it('should open and read a file stream', function (done) {
    var remoteFileStream = hdfs.createReadStream(path + '/file-1');
    var spy = sinon.spy();
    var data = [];

    remoteFileStream.on('error', spy);
    remoteFileStream.on('data', function onData (chunk) {
      data.push(chunk);
    });

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();
      demand(Buffer.concat(data).toString()).be.equal('random datamore random data');

      done();
    });
  });

  it('should open and read a file', function (done) {
    hdfs.readFile(path + '/file-1', function (err, data) {
      demand(err).be.null();
      demand(data.toString()).be.equal('random datamore random data');
      done();
    });
  });

  it('should list directory status', function (done) {
    hdfs.readdir(path, function (err, files) {
      demand(err).be.null();
      demand(files).have.length(2);

      demand(files[0].pathSuffix).to.eql('file-1');
      demand(files[1].pathSuffix).to.eql('file-2');

      demand(files[0].type).to.eql('FILE');
      demand(files[1].type).to.eql('FILE');
      done();
    });
  });

  it('should change file permissions', function (done) {
    hdfs.chmod(path, '0777', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should change file owner', function (done) {
    hdfs.chown(path, user, 'supergroup', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should rename file', function (done) {
    hdfs.rename(path+ '/file-2', path + '/bigfile', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should check file existence', function (done) {
    hdfs.exists(path + '/bigfile', function (exists) {
      demand(exists).be.true();

      done();
    });
  });

  it('should stat file', function (done) {
    hdfs.stat(path + '/bigfile', function (err, stats) {
      demand(err).be.null();
      demand(stats).be.object();

      demand(stats.type).to.eql('FILE');
      demand(stats.owner).to.eql(user);

      done();
    });
  });

  it('should create symbolic link', function (done) {
    hdfs.symlink(path+ '/bigfile', path + '/biggerfile', function (err) {
      // Pass if server doesn't support symlinks
      if (err && err.message.indexOf('Symlinks not supported') !== -1) {
        done();
      } else {
        demand(err).be.null();
        done();
      }
    });
  });

  it('should delete file', function (done) {
    hdfs.rmdir(path+ '/file-1', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should delete directory recursively', function (done) {
    hdfs.rmdir(path, true, function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should support optional opts', function (done) {
    var myOpts = {
      "user.name": "testuser"
    }
    hdfs.writeFile(path + '/file-1', 'random data', myOpts, function (err) {
      demand(err).be.null();
      done();
    });
  });

});

describe('WebHDFS with requestParams', function() {
  var path = '/files/' + Math.random();
  var hdfs = WebHDFS.createClient({
    user: user,
    port: 45001
  }, {
    headers: {
      'X-My-Custom-Header': 'Kerberos'
    }
  });

  this.timeout(10000);

  before(function (done) {
    var opts = {
      path: '/webhdfs/v1',
      http: {
        port: 45001
      }
    };

    WebHDFSProxy.createServer(opts, WebHDFSProxyMemoryStorage, done);
  });

  it('should override request() options', function (done) {
    var localFileStream = fs.createReadStream(__filename);
    var remoteFileStream = hdfs.createWriteStream(path + '/file-2');
    var spy = sinon.spy();

    localFileStream.pipe(remoteFileStream);
    remoteFileStream.on('error', spy);

    remoteFileStream.on('response', function(response) {
      var customHeader = response.req.getHeader('X-My-Custom-Header');
      demand(customHeader).equal('Kerberos');
      demand(spy.called).be.falsy();
      done();
    })

  });

  it('should pass requestParams to _sendRequest', function (done) {
    var req = hdfs.readdir('/');

    req.on('response', function(response) {
      var customHeader = response.req.getHeader('X-My-Custom-Header');
      demand(customHeader).equal('Kerberos');
      done();
    });
  });

  it('should not override explicit opts with _sendRequest', function (done) {
    var mostSpecificParams = {
      headers: {
        'X-My-Custom-Header': 'Bear'
      }
    }

    var endpoint = hdfs._getOperationEndpoint('liststatus', '/file-2');

    hdfs._sendRequest('GET', endpoint, mostSpecificParams, function(err, response, body) {
      var customHeader = response.req.getHeader('X-My-Custom-Header');
      demand(customHeader).equal('Bear');
      done(err)
    });
  });

});

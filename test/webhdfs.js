var fs = require('fs');
var must = require('must');
var demand = must;
var sinon = require('sinon');

var WebHDFS = require('../lib/webhdfs');

describe('WebHDFS', function () {
  var path = '/files/' + Math.random();
  var hdfs = WebHDFS.createClient({
    user: process.env.USER
  });

  it('should make a directory', function (done) {
    hdfs.mkdir(path, function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should list directory status', function () {});

  it('should create and write data to a file', function (done) {
    hdfs.writeFile(path + '-1', 'random data', function () {
      done();
    });
  });

  it('should create and stream data to a file', function (done) {
    var localFileStream = fs.createReadStream(__filename);
    var remoteFileStream = hdfs.createWriteStream(path + '-2');
    var spy = sinon.spy();

    localFileStream.pipe(remoteFileStream);
    remoteFileStream.on('error', spy);
    remoteFileStream.on('error', function (err) {
      console.log(err);
    });

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();
      done();
    });
  });

  it('should open and read a file', function () {});
  it('should append content to an existing file', function () {});
  it('should change file permissions', function () {});
  it('should create symlink to file', function () {});
  it('should rename file', function () {});
  it('should delete file', function () {});

  it('should delete directory', function (done) {
    hdfs.rmdir('/tmp/path', true, function (err) {
      demand(err).be.null();
      done();
    });
  });
});
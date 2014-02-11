var must = require('must');
var demand = must;

var WebHDFS = require('../lib/webhdfs');

describe('WebHDFS', function () {
  var fs = WebHDFS.createClient({
    user: process.env.USER
  });

  it('should make a directory', function (done) {
    fs.mkdir('/tmp/path', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should list directory status', function () {});
  it('should create and write to a file', function () {});
  it('should open and read a file', function () {});
  it('should append content to an existing file', function () {});
  it('should change file permissions', function () {});
  it('should create symlink to file', function () {});
  it('should rename file', function () {});
  it('should delete file', function () {});

  it('should delete directory', function (done) {
    fs.rmdir('/tmp/path', function (err) {
      demand(err).be.null();
      done();
    });
  });
});
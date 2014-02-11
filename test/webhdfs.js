var must = require('must');
var demand = must;

var WebHDFS = require('../lib/webhdfs');

describe('WebHDFS', function () {
  var fs = WebHDFS.createClient({
    user: process.env.USER
  });

  it('should make a directory', function (done) {
    fs.mkdir('/temp', function () {
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
  it('should delete directory', function () {
    fs.unlink('/temp', function () {
      done();
    });
  });
});
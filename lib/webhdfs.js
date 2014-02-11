var extend = require('extend');
var util = require('util');
var url = require('url');
var querystring = require('querystring');
var request = require('request');

function WebHDFS (opts) {
  if (!(this instanceof WebHDFS)) {
    return new WebHDFS(opts);
  }

  [ 'user', 'host', 'port', 'path' ].some(function iterate (property) {
    if (!opts.hasOwnProperty(property)) {
      throw new Error(
        util.format('Unable to create WebHDFS client: missing option %s', property)
      );
    }
  });

  this._opts = opts;
  this._url = {
    protocol: 'http',
    hostname: opts.host,
    port: parseInt(opts.port) || 80,
    pathname: opts.path
  };
}

WebHDFS.prototype._getOperationEndpoint = function _getOperationEndpoint (operation, path, params) {
  var endpoint = this._url;

  endpoint.pathname += path + '/';
  endpoint.search = querystring.stringify(extend({
    'op': operation,
    'user.name': this._opts.user
  }, params));

  return url.format(endpoint);
};

WebHDFS.prototype._sendRequest = function _sendRequest (method, url, opts, callback) {
  if (typeof callback === 'undefined') {
    callback = opts;
    opts = {};
  }

  return request(extend({
    method: method,
    url: url,
    json: true
  }, opts), function onComplete(err, res, body) {
    if (err) {
      return callback(err);
    }

    // Handle remote exceptions
    if (res.statusCode !== 200) {
      if (body.hasOwnProperty('RemoteException')) {
        body = body.RemoteException;
      } else {
        body = {
          message: 'Unknown error'
        }
      }

      return callback(new Error(body.message));
    }

    callback(err, res, body);
  });
};

WebHDFS.prototype.chmod = function writeFile (path, mode, callback) {

};

WebHDFS.prototype.mkdir = function writeFile (path, mode, callback) {
  if (typeof callback === 'undefined') {
    callback = mode;
    mode = null;
  }

  var endpoint = this._getOperationEndpoint('mkdirs', path, {
    permissions: mode || '0677'
  });

  var req = this._sendRequest('PUT', endpoint, function (err) {
    console.log(arguments);
  });
};

WebHDFS.prototype.writeFile = function writeFile (path) {

};

module.exports = {
  createClient: function createClient (opts) {
    return new WebHDFS(extend({
      user: 'webuser',
      host: 'localhost',
      port: '50070',
      path: '/webhdfs/v1'
    }, opts));
  }
};
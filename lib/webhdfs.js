var extend = require('extend');
var util = require('util');
var url = require('url');
var querystring = require('querystring');
var request = require('request');
var StringReader = require('./string-reader');

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

  endpoint.pathname = this._opts.path + path;
  endpoint.search = querystring.stringify(extend({
    'op': operation,
    'user.name': this._opts.user
  }, params || {}));

  return url.format(endpoint);
};

WebHDFS.prototype._parseError = function _parseError (body) {
  var error = null;

  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch (err) {
      body = null;
    }
  }

  if (body && body.hasOwnProperty('RemoteException')) {
    error = body.RemoteException;
  } else {
    error = {
      message: 'Unknown error'
    };
  }

  return new Error(error.message);
};

WebHDFS.prototype._sendRequest = function _sendRequest (method, url, opts, callback) {
  if (typeof callback === 'undefined') {
    callback = opts;
    opts = {};
  }

  var self = this;
  return request(extend({
    method: method,
    url: url,
    json: true
  }, opts), function onComplete(err, res, body) {
    if (err) {
      return callback(err);
    }

    // Handle remote exceptions
    if (res.statusCode >= 400) {
      return callback(self._parseError(body));
    } else if (res.statusCode >= 200 && res.statusCode <= 300) {
      return callback(err, res, body);
    } else {
      return callback(new Error('Unexpected redirect'), res, body);
    }
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

  return this._sendRequest('PUT', endpoint, function (err) {
    callback(err);
  });
};

WebHDFS.prototype.writeFile = function writeFile (path, data, opts, callback) {
  if (typeof callback === 'undefined') {
    callback = opts;
    opts = {};
  }

  var error = null;
  var localStream = new StringReader(data);
  var remoteStream = this.createWriteStream(path, opts);

  localStream.pipe(remoteStream); // Pipe data

  // Handle events
  remoteStream.on('error', function onError (err) {
    error = err;
  });

  remoteStream.on('finish', function onFinish () {
    return callback(error);
  });

  return remoteStream;
};

WebHDFS.prototype.createWriteStream = function createWriteStream (path, opts) {
  var endpoint = this._getOperationEndpoint('create', path, extend({
    overwrite: true,
    permissions: '0677'
  }, opts));

  var self = this;
  var stream = null;
  var params = {
    method: 'PUT',
    url: endpoint,
    json: true
  };

  var req = request(params, function (err, res, body) {
    if (err) {
      return callback(err);
    }

    // Handle redirect
    if (res.statusCode === 307 &&
      res.headers.hasOwnProperty('location')) {

      var upload = request(extend(params, { url: res.headers['location'] }), function (err, res, body) {
        if (err) {
          req.emit('error', err);
        }

        // Handle remote exceptions
        if (res.statusCode >= 400) {
          req.emit('error', self._parseError(body));
        }

        req.emit('finish'); // Request is finished
      });

      stream.pipe(upload);
      stream.resume();
    }
  });

  req.on('pipe', function (src) {
    // Unpipe initial request
    src.unpipe(req);
    req.end();

    // Pause read stream
    stream = src;
    stream.pause();
  });

  return req;
};

WebHDFS.prototype.unlink = function writeFile (path, recursive, callback) {
  if (typeof callback === 'undefined') {
    callback = recursive;
    recursive = null;
  }

  var endpoint = this._getOperationEndpoint('delete', path, {
    recursive: recursive || false
  });

  return this._sendRequest('DELETE', endpoint, function (err) {
    callback(err);
  });
};

WebHDFS.prototype.rmdir = WebHDFS.prototype.unlink;

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
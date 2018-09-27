/**
 * @module WebHDFS
 */

var extend = require('extend');
var util = require('util');
var $url = require('url');
var querystring = require('querystring');
var request = require('request');
var BufferStreamReader = require('buffer-stream-reader');

/**
 * Initializes new WebHDFS instance
 *
 * @param {Object} [opts]
 * @param {Object} [requestParams]
 * @returns {WebHDFS}
 *
 * @constructor
 */
function WebHDFS (opts, requestParams) {
  if (!(this instanceof WebHDFS)) {
    return new WebHDFS(opts, requestParams);
  }

  [ 'user', 'host', 'port', 'path' ].some(function iterate (property) {
    if (!opts.hasOwnProperty(property)) {
      throw new Error(
        util.format('Unable to create WebHDFS client: missing option %s', property)
      );
    }
  });

  this._requestParams = requestParams;

  if(!this._requestParams)
      this._requestParams = {};

  if( ! ('forever' in this._requestParams) )
    this._requestParams.forever = true;

  var hosts = [];
  opts.host.split(',').forEach(function(host){ // comma separated hosts for HA support
    hosts.push(host.trim(''));
  });
  opts.host = hosts;

  this._opts = opts;
  this._curr_host_idx = 0;
  this._url = {
    protocol: opts.protocol || 'http',
    hostname: opts.host[this._curr_host_idx], // pick one host to startwith
    port: parseInt(opts.port) || 80,
    pathname: opts.path
  };
}

/**
 * Generate WebHDFS REST API endpoint URL for given operation
 *
 * @method _getOperationEndpoint
 *
 * @param {String} operation WebHDFS operation name
 * @param {String} path
 * @param {Object} params
 *
 * @returns {String}
 * @private
 */
WebHDFS.prototype._getOperationEndpoint = function _getOperationEndpoint (operation, path, params) {
  var endpoint = this._url;

  endpoint.pathname = this._opts.path + path;
  endpoint.search = querystring.stringify(extend({
    'op': operation,
    'user.name': this._opts.user
  }, params || {}));

  return $url.format(endpoint);
};

/**
 * Parse 'RemoteException' structure and return valid Error object
 *
 * @method _parseError
 *
 * @param {String|Object} body Response body
 * @param {Boolean} strict If set true then RemoteException must be present in the body
 * @returns {Error}
 * @private
 */
WebHDFS.prototype._parseError = function _parseError (body, strict) {
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
    if (!strict) {
      error = {
        message: 'Unknown error'
      };
    }
  }

  return error ? error : null;
};

/**
 * Check if response is redirect
 *
 * @method _isRedirect
 *
 * @param res
 * @returns {Boolean}
 * @private
 */
WebHDFS.prototype._isRedirect = function _isRedirect (res) {
  return [ 301, 307 ].indexOf(res.statusCode) !== -1 &&
    res.headers.hasOwnProperty('location');
};

/**
 * Check if response is successful
 *
 * @method _isSuccess
 *
 * @param res
 * @returns {Boolean}
 * @private
 */
WebHDFS.prototype._isSuccess = function _isRedirect (res) {
  return [ 200, 201 ].indexOf(res.statusCode) !== -1;
};

/**
 * Check if response is error
 *
 * @method _isError
 *
 * @param res
 * @returns {Boolean}
 * @private
 */
WebHDFS.prototype._isError = function _isRedirect (res) {
  return [ 400, 401, 402, 403, 404, 500 ].indexOf(res.statusCode) !== -1;
};

/**
 * Send a request to WebHDFS REST API
 *
 * @method _sendRequest
 *
 * @param {String} method HTTP method
 * @param {String} url
 * @param {Object} opts Options for request
 * @param {Function} callback
 *
 * @returns {Object} request instance
 *
 * @private
 */
WebHDFS.prototype._sendRequest = function _sendRequest (method, url, opts, callback) {
  if (typeof callback === 'undefined')
  {
    callback = opts;
    opts = {};
  }

  var self      = this;
  var params    = extend({ method: method, url: url, json: true }, this._requestParams, opts);
  var req       = request(params, onComplete);

  function onComplete(err, res, body)
  {
    if (err && err.code !== 'ECONNREFUSED')
    {
        return callback && callback(err);
    }

    // Handle remote exceptions and `ECONNREFUSED`
    if ((res && self._isError(res)) || (err && err.code === 'ECONNREFUSED'))
    {
      if(
            self._opts.host.length > 1 &&
            ((body && body.RemoteException && body.RemoteException.exception === 'StandbyException')||
             (err && err.code === 'ECONNREFUSED'))
        )
      {
        if(!opts._retry)
        {
          self._curr_host_idx   = !self._curr_host_idx + 0;
          self._url.hostname    = self._opts.host[self._curr_host_idx];
          var urlJSON           = $url.parse(url);
          urlJSON.hostname      = self._url.hostname;
          delete urlJSON.host;
          url                   = $url.format(urlJSON);
          setTimeout(_sendRequest.bind(self), 0, method, url, extend({ _retry: true }, opts), callback);
          return;
        }
        else
        {
          return callback && callback(self._parseError(body));
        }
      }
      else
        return callback && callback(self._parseError(body));
    }
    else if (self._isSuccess(res))
    {
      return callback && callback(err, res, body);
    }
    else
    {
      return callback && callback(new Error('Unexpected redirect'), res, body);
    }
  };

  return req;
};

/**
 * Change file permissions
 *
 * @method chmod
 *
 * @param {String} path
 * @param {String} mode
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.chmod = function chmod (path, mode, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('setpermission', path, extend({
    permission: mode
  }));

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Change file owner
 *
 * @method chown
 *
 * @param {String} path
 * @param {String} uid User name
 * @param {String} gid Group name
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.chown = function chown (path, uid, gid, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('setowner', path, extend({
    owner: uid,
    group: gid
  }));

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Read directory contents
 *
 * @method _readdir
 *
 * @param {String} path
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.readdir = function readdir (path, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('liststatus', path);
  return this._sendRequest('GET', endpoint, function (err, res, body) {
    if (err) {
      return callback && callback(err);
    }

    var files = [];
    if (res.body.hasOwnProperty('FileStatuses') &&
        res.body.FileStatuses.hasOwnProperty('FileStatus')) {

      files = res.body.FileStatuses.FileStatus;
      return callback && callback(null, files);
    } else {
      return callback && callback(new Error('Invalid data structure'));
    }
  });
};

/**
 * Make new directory
 *
 * @method mkdir
 *
 * @param {String} path
 * @param {String} [mode=0777]
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.mkdir = function mkdir (path, mode, callback) {
  if (typeof callback === 'undefined') {
    callback = mode;
    mode = null;
  }

  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('mkdirs', path, {
    permissions: mode || '0777'
  });

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Rename path
 *
 * @method rename
 *
 * @param {String} path
 * @param {String} destination
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.rename = function rename (path, destination, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('rename', path, extend({
    destination: destination
  }));

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Stat path
 *
 * @method stat
 *
 * @param {String} path
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.stat = function stat (path, callback) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('getfilestatus', path);
  return this._sendRequest('GET', endpoint, function (err, res) {
    if (err) {
      return callback && callback(err);
    }

    if (res.body.hasOwnProperty('FileStatus')) {
      return callback && callback(null, res.body.FileStatus);
    } else {
      return callback && callback(new Error('Invalid data structure'));
    }
  });
};

/**
 * Check file existence
 * Wraps stat method
 *
 * @method stat
 * @see WebHDFS.stat
 *
 * @param {String} path
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.exists = function stat (path, callback) {
  return this.stat(path, function onStatResult (err, stats) {
    return callback(err || !stats ? false : true);
  });
};

/**
 * Write data to the file
 *
 * @method writeFile
 *
 * @param {String} path
 * @param {Buffer|String} data
 * @param {Boolean} [append] If set to true then append data to the file
 * @param {Object} [opts]
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.writeFile = function writeFile (path, data, append, opts, callback) {
  if (typeof append === 'function') {
    callback = append;
    append = false;
    opts = {};
  } else if (typeof append === 'object') {
    callback = opts;
    opts = append;
    append = false;
  } else if (typeof opts === 'function') {
    callback = opts;
    opts = {};
  }

  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var error = null;
  var localStream = new BufferStreamReader(data);
  var remoteStream = this.createWriteStream(path, append, opts);

  // Handle events
  remoteStream.once('error', function onError (err) {
    error = err;
  });

  remoteStream.once('finish', function onFinish () {
    return callback && callback(error);
  });

  localStream.pipe(remoteStream); // Pipe data

  return remoteStream;
};

/**
 * Append data to the file
 *
 * @see writeFile
 * @param {String} path
 * @param {Buffer|String} data
 * @param {Object} [opts]
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.appendFile = function appendFile (path, data, opts, callback) {
  return this.writeFile(path, data, true, opts, callback);
};

/**
 * Read data from the file
 *
 * @method readFile
 *
 * @fires Request#data
 * @fires WebHDFS#finish
 *
 * @param {String} path
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.readFile = function readFile (path, callback) {
  var remoteFileStream = this.createReadStream(path);
  var data = [];
  var error = null;

  remoteFileStream.once('error', function onError (err) {
    error = err;
  });

  remoteFileStream.on('data', function onData (chunk) {
    data.push(chunk);
  });

  remoteFileStream.once('finish', function () {
    if (!error) {
      return callback && callback(null, Buffer.concat(data));
    } else {
      return callback && callback(error);
    }
  });
};

/**
 * Create writable stream for given path
 *
 * @example
 *
 * var WebHDFS = require('webhdfs');
 * var hdfs = WebHDFS.createClient();
 *
 * var localFileStream = fs.createReadStream('/path/to/local/file');
 * var remoteFileStream = hdfs.createWriteStream('/path/to/remote/file');
 *
 * localFileStream.pipe(remoteFileStream);
 *
 * remoteFileStream.on('error', function onError (err) {
 *   // Do something with the error
 * });
 *
 * remoteFileStream.on('finish', function onFinish () {
 *  // Upload is done
 * });
 *
 * @method createWriteStream
 * @fires WebHDFS#finish
 *
 * @param {String} path
 * @param {Boolean} [append] If set to true then append data to the file
 * @param {Object} [opts]
 *
 * @returns {Object}
 */
WebHDFS.prototype.createWriteStream = function createWriteStream (path, append, opts) {
  if (typeof append === 'object') {
    opts = append;
    append = false;
  }

  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var emitError = function (instance, err) {
    const isErrorEmitted = instance.errorEmitted;

    if (!isErrorEmitted) {
      instance.emit('error', err);
      instance.emit('finish');
    }

    instance.errorEmitted = true;
  };

  var endpoint = this._getOperationEndpoint(append ? 'append' : 'create', path, extend({
    overwrite: true,
    permissions: '0777'
  }, opts));

  var self = this;
  var stream = null;
  var canResume = true;
  var params = extend({
    method: append ? 'POST' : 'PUT',
    url: endpoint,
    json: true,
    headers: { 'content-type': 'application/octet-stream' }
  }, this._requestParams);

  var req = request(params, function (err, res, body) {
    // Handle redirect only if there was not an error (e.g. res is defined)
    if (res && self._isRedirect(res)) {
      var upload = request(extend(params, { url: res.headers.location }), function (err, res, body) {
        if (err || self._isError(res)) {
          emitError(req, err || self._parseError(body));
          req.end();
          return;
        }

        if (res.headers.hasOwnProperty('location')) {
          return req.emit('finish', res.headers.location);
        } else {
          return req.emit('finish');
        }
      });

      canResume = true; // Enable resume

      stream.pipe(upload);
      stream.resume();
    }

    if (err || self._isError(res)) {
      emitError(req, err || self._parseError(body));
      return;
    }
  });

  req.on('pipe', function onPipe (src) {
    // Pause read stream
    stream = src;
    stream.pause();

    // This is not an elegant solution but here we go
    // Basically we don't allow pipe() method to resume reading input
    // and set internal _readableState.flowing to false
    canResume = false;
    stream.on('resume', function () {
      if (!canResume) {
        stream._readableState.flowing = false;
      }
    });

    // Unpipe initial request
    src.unpipe(req);
    req.end();
  });

  return req;
};

function _doRetry(self, req, params){
    var download = request(params);
    download.on('complete', function (err) {
        req.emit('end');
    });

    // Proxy data to original data handler
    download.on('data', function onData (chunk) {
        var error = self._parseError(chunk.toString(), true);
        if(!error)
            req.emit('data', chunk);
    });

    // Handle subrequest
    download.on('response', function onResponse (res) {
      if (self._isError(res)) {
        download.on('data', function onData (data) {
          var error = self._parseError(data.toString());
          error.message     = 'No active namenode found';
          error.exception   = 'NoActiveNamenodeException'; // to avoid recursively error emitting loop
          req.emit('error', error);
          req.end();
        });
      }
    });
}

/**
 * Create readable stream for given path
 *
 * @example
 * var WebHDFS = require('webhdfs');
 * var hdfs = WebHDFS.createClient();
 *
 * var remoteFileStream = hdfs.createReadStream('/path/to/remote/file');
 *
 * remoteFileStream.on('error', function onError (err) {
 *  // Do something with the error
 * });
 *
 * remoteFileStream.on('data', function onChunk (chunk) {
 *  // Do something with the data chunk
 * });
 *
 * remoteFileStream.on('finish', function onFinish () {
 *  // Upload is done
 * });
 *
 * @method createReadStream
 * @fires Request#data
 * @fires WebHDFS#finish
 *
 * @param {String} path
 * @param {Object} [opts]
 *
 * @returns {Object}
 */
WebHDFS.prototype.createReadStream = function createReadStream (path, opts) {
  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var self = this;
  var endpoint = this._getOperationEndpoint('open', path, opts);
  var stream = null;
  var params = extend({
    method: 'GET',
    url: endpoint,
    json: true
  }, this._requestParams);

  var req = request(params);

  req.on('error', function(error){
    if(self._opts.host.length > 1 && error.code === 'ECONNREFUSED') // retry iff namenode daemon is down
    {
      self._curr_host_idx = !self._curr_host_idx + 0;
      self._url.hostname  = self._opts.host[self._curr_host_idx];
      params.url          = self._getOperationEndpoint('open', path, opts);
      console.log('Retrying with url: '+ params.url);
      _doRetry(self, req, params);
    }
  });

  req.on('response', function (res) {
    // Handle remote exceptions
    if (self._isError(res)) {
      req.on('data', function onData (data) {
        var error = self._parseError(data.toString(), true);
        if(error)
        {
          if(self._opts.host.length < 2 || // retry iff multiple namenode hosts are available
             error.exception !== 'StandbyException')
          {
            req.emit('error', error);
            req.end();
          }
          else
          {
            self._curr_host_idx = !self._curr_host_idx + 0;
            self._url.hostname  = self._opts.host[self._curr_host_idx];
            params.url          = self._getOperationEndpoint('open', path, opts);
            console.log('Retrying with url: '+ params.url);
            _doRetry(self, req, params);
          }
        }
      });
    } else if (self._isRedirect(res)) {
        _doRetry(self, req, params);
    }

    // No need to interrupt the request
    // data will be automatically sent to the data handler
  });

  return req;
};

/**
 * Create symbolic link to the destination path
 *
 * @method symlink
 *
 * @param {String} src
 * @param {String} dest
 * @param {Boolean} [createParent=false]
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.symlink = function symlink (src, dest, createParent, callback) {
  if (typeof createParent === 'function') {
    callback = createParent;
    createParent = false;
  }

  // Validate path
  if (!src || typeof src !== 'string') {
    throw new Error('src path must be a string');
  }

  if (!dest || typeof dest !== 'string') {
    throw new Error('dest path must be a string');
  }

  var endpoint = this._getOperationEndpoint('createsymlink', src, {
    createParent: createParent || false,
    destination: dest
  });

  return this._sendRequest('PUT', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * Unlink path
 *
 * @method unlink
 *
 * @param {String} path
 * @param {Boolean} [recursive=false]
 * @param {Function} callback
 *
 * @returns {Object}
 */
WebHDFS.prototype.unlink = function unlink (path, recursive, callback) {
  if (typeof callback === 'undefined') {
    callback = recursive;
    recursive = null;
  }

  // Validate path
  if (!path || typeof path !== 'string') {
    throw new Error('path must be a string');
  }

  var endpoint = this._getOperationEndpoint('delete', path, {
    recursive: recursive || false
  });

  return this._sendRequest('DELETE', endpoint, function (err) {
    return callback && callback(err);
  });
};

/**
 * @alias WebHDFS.unlink
 */
WebHDFS.prototype.rmdir = WebHDFS.prototype.unlink;

module.exports = {
  createClient: function createClient (opts, requestParams) {
    return new WebHDFS(extend({
      user: 'webuser',
      host: 'localhost',
      port: '50070',
      path: '/webhdfs/v1'
    }, opts), requestParams);
  }
};

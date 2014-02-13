var util = require('util');
var stream = require('stream');

function StringReader (data, options) {
  if (!(this instanceof StringReader)) {
    return new StringReader(data);
  }

  stream.Readable.call(this, options);

  this._data = null;

  if (typeof data === 'string') {
    this._data = new Buffer(data, options.encoding || 'utf8');
  } else if (Buffer.isBuffer(data)) {
    this._data = data;
  }
}

StringReader.prototype = Object.create(stream.Readable.prototype, {
  constructor: { value: StringReader }
});

StringReader.prototype._read = function _read (size) {
  if (!this._data) {
    return this.push(null);
  }

  var canRead = true;
  var dataLength = this._data.length;
  var bytesToRead = dataLength;
  var chunkSize = size || 1024 * 100;

  while (canRead) {
    var buf = this._data.slice(0, Math.min(chunkSize, bytesToRead));

    this.push(buf);
    bytesToRead -= chunkSize;

    if (bytesToRead <= 0) {
      canRead = false;
      return this.push(null);
    }

    this._data = this._data.slice(bytesToRead);
  }
};

module.exports = StringReader;
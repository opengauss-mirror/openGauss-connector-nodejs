/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

'use strict'
var helper = require('./../test-helper')

if (helper.args.native) {
  Client = require('./../../lib/native')
  helper.Client = Client
  helper.pg = helper.pg.native
}

// creates a client from cli parameters
helper.client = function (cb) {
  var client = new Client()
  client.connect(cb)
  return client
}

helper.versionGTE = function (client, testVersion, callback) {
  client.query(
    'SHOW server_version_num',
    assert.calls(function (err, result) {
      if (err) return callback(err)
      var version = parseInt(result.rows[0].server_version_num, 10)
      return callback(null, version >= testVersion)
    })
  )
}

// export parent helper stuffs
module.exports = helper

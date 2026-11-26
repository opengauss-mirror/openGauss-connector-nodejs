/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict'
var helper = require('./test-helper')
var Client = helper.Client

var suite = new helper.Suite()

var conInfo = helper.config

function getConInfo(override) {
  return Object.assign({}, conInfo, override)
}

function getAppName(conf, cb) {
  var client = new Client(conf)
  client.connect(
    assert.success(function () {
      client.query(
        'SHOW application_name',
        assert.success(function (res) {
          var appName = res.rows[0].application_name
          cb(appName)
          client.end()
        })
      )
    })
  )
}

suite.test('No default appliation_name ', function (done) {
  var conf = getConInfo()
  getAppName({}, function (res) {
    assert.strictEqual(res, '')
    done()
  })
})

suite.test('fallback_application_name is used', function (done) {
  var fbAppName = 'this is my app'
  var conf = getConInfo({
    fallback_application_name: fbAppName,
  })
  getAppName(conf, function (res) {
    assert.strictEqual(res, fbAppName)
    done()
  })
})

suite.test('application_name is used', function (done) {
  var appName = 'some wired !@#$% application_name'
  var conf = getConInfo({
    application_name: appName,
  })
  getAppName(conf, function (res) {
    assert.strictEqual(res, appName)
    done()
  })
})

suite.test('application_name has precedence over fallback_application_name', function (done) {
  var appName = 'some wired !@#$% application_name'
  var fbAppName = 'some other strange $$test$$ appname'
  var conf = getConInfo({
    application_name: appName,
    fallback_application_name: fbAppName,
  })
  getAppName(conf, function (res) {
    assert.strictEqual(res, appName)
    done()
  })
})

suite.test('application_name from connection string', function (done) {
  var appName = 'my app'
  var conParams = require('../../../lib/connection-parameters')
  var conf
  if (process.argv[2]) {
    conf = new conParams(process.argv[2] + '?application_name=' + appName)
  } else {
    conf = 'postgres://?application_name=' + appName
  }
  getAppName(conf, function (res) {
    assert.strictEqual(res, appName)
    done()
  })
})

// TODO: make the test work for native client too
if (!helper.args.native) {
  suite.test('application_name is read from the env', function (done) {
    var appName = (process.env.PGAPPNAME = 'testest')
    getAppName({}, function (res) {
      delete process.env.PGAPPNAME
      assert.strictEqual(res, appName)
      done()
    })
  })
}

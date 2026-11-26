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
var helper = require('./../test-helper')

//native bindings are only installed for native tests
if (!helper.args.native) {
  return
}

var assert = require('assert')
var pg = require('../../../lib')
var native = require('../../../lib').native

var JsClient = require('../../../lib/client')
var NativeClient = require('../../../lib/native')

assert(pg.Client === JsClient)
assert(native.Client === NativeClient)

const jsPool = new pg.Pool()
const nativePool = new native.Pool()

const suite = new helper.Suite()
suite.test('js pool returns js client', (cb) => {
  jsPool.connect(function (err, client, done) {
    assert(client instanceof JsClient)
    done()
    jsPool.end(cb)
  })
})

suite.test('native pool returns native client', (cb) => {
  nativePool.connect(function (err, client, done) {
    assert(client instanceof NativeClient)
    done()
    nativePool.end(cb)
  })
})

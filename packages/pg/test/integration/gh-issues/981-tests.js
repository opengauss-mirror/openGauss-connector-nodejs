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

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
const assert = require('assert')
const helper = require('./../test-helper')
const suite = new helper.Suite()
const pg = require('../../../lib/index')
const Client = pg.Client

const password = process.env.PGPASSWORD || null
const sleep = (millis) => new Promise((resolve) => setTimeout(resolve, millis))

if (!password) {
  // skip these tests; no password will be requested
  return
}

suite.testAsync('Get password from a sync function', () => {
  let wasCalled = false
  function getPassword() {
    wasCalled = true
    return password
  }
  const client = new Client({
    password: getPassword,
  })
  return client.connect().then(() => {
    assert.ok(wasCalled, 'Our password function should have been called')
    return client.end()
  })
})

suite.testAsync('Throw error from a sync function', () => {
  let wasCalled = false
  const myError = new Error('Oops!')
  function getPassword() {
    wasCalled = true
    throw myError
  }
  const client = new Client({
    password: getPassword,
  })
  let wasThrown = false
  return client
    .connect()
    .catch((err) => {
      assert.equal(err, myError, 'Our sync error should have been thrown')
      wasThrown = true
    })
    .then(() => {
      assert.ok(wasCalled, 'Our password function should have been called')
      assert.ok(wasThrown, 'Our error should have been thrown')
      return client.end()
    })
})

suite.testAsync('Get password from a function asynchronously', () => {
  let wasCalled = false
  function getPassword() {
    wasCalled = true
    return sleep(100).then(() => password)
  }
  const client = new Client({
    password: getPassword,
  })
  return client.connect().then(() => {
    assert.ok(wasCalled, 'Our password function should have been called')
    return client.end()
  })
})

suite.testAsync('Throw error from an async function', () => {
  let wasCalled = false
  const myError = new Error('Oops!')
  function getPassword() {
    wasCalled = true
    return sleep(100).then(() => {
      throw myError
    })
  }
  const client = new Client({
    password: getPassword,
  })
  let wasThrown = false
  return client
    .connect()
    .catch((err) => {
      assert.equal(err, myError, 'Our async error should have been thrown')
      wasThrown = true
    })
    .then(() => {
      assert.ok(wasCalled, 'Our password function should have been called')
      assert.ok(wasThrown, 'Our error should have been thrown')
      return client.end()
    })
})

suite.testAsync('Password function must return a string', () => {
  let wasCalled = false
  function getPassword() {
    wasCalled = true
    // Return a password that is not a string
    return 12345
  }
  const client = new Client({
    password: getPassword,
  })
  let wasThrown = false
  return client
    .connect()
    .catch((err) => {
      assert.ok(err instanceof TypeError, 'A TypeError should have been thrown')
      assert.equal(err.message, 'Password must be a string')
      wasThrown = true
    })
    .then(() => {
      assert.ok(wasCalled, 'Our password function should have been called')
      assert.ok(wasThrown, 'Our error should have been thrown')
      return client.end()
    })
})

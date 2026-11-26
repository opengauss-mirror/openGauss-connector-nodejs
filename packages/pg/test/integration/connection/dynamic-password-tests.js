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

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

const suite = new helper.Suite()

suite.test('calling end during active query should return a promise', (done) => {
  const client = new helper.pg.Client()
  let callCount = 0
  // ensure both the query rejects and the end promise resolves
  const after = () => {
    if (++callCount > 1) {
      done()
    }
  }
  client.connect().then(() => {
    client.query('SELECT NOW()').catch(after)
    client.end().then(after)
  })
})

suite.test('calling end during an active query should call end callback', (done) => {
  const client = new helper.pg.Client()
  let callCount = 0
  // ensure both the query rejects and the end callback fires
  const after = () => {
    if (++callCount > 1) {
      done()
    }
  }
  client.connect().then(() => {
    client.query('SELECT NOW()').catch(after)
    client.end(after)
  })
})

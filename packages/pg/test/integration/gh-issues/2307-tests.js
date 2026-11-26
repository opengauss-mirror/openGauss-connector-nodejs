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

const pg = require('../../../lib')
const helper = require('../test-helper')

const suite = new helper.Suite()

suite.test('bad ssl credentials do not cause crash', (done) => {
  const config = {
    ssl: {
      ca: 'invalid_value',
      key: 'invalid_value',
      cert: 'invalid_value',
    },
  }

  const client = new pg.Client(config)

  client.connect((err) => {
    assert(err)
    client.end()
    done()
  })
})

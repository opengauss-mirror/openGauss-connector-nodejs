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
var helper = require('./test-helper')
var Query = require('../../../lib/query')

var client = helper.client()
var con = client.connection
var parseArg = null
con.parse = function (arg) {
  parseArg = arg
  process.nextTick(function () {
    con.emit('parseComplete')
  })
}

var bindArg = null
con.bind = function (arg) {
  bindArg = arg
  process.nextTick(function () {
    con.emit('bindComplete')
  })
}

var executeArg = null
con.execute = function (arg) {
  executeArg = arg
  process.nextTick(function () {
    con.emit('rowData', { fields: [] })
    con.emit('commandComplete', { text: '' })
  })
}

var describeArg = null
con.describe = function (arg) {
  describeArg = arg
  process.nextTick(function () {
    con.emit('rowDescription', { fields: [] })
  })
}

var syncCalled = false
con.flush = function () {}
con.sync = function () {
  syncCalled = true
  process.nextTick(function () {
    con.emit('readyForQuery')
  })
}

test('bound command', function () {
  test('simple, unnamed bound command', function () {
    assert.ok(client.connection.emit('readyForQuery'))

    var query = client.query(
      new Query({
        text: 'select * from X where name = $1',
        values: ['hi'],
      })
    )

    assert.emits(query, 'end', function () {
      test('parse argument', function () {
        assert.equal(parseArg.name, null)
        assert.equal(parseArg.text, 'select * from X where name = $1')
        assert.equal(parseArg.types, null)
      })

      test('bind argument', function () {
        assert.equal(bindArg.statement, null)
        assert.equal(bindArg.portal, '')
        assert.lengthIs(bindArg.values, 1)
        assert.equal(bindArg.values[0], 'hi')
      })

      test('describe argument', function () {
        assert.equal(describeArg.type, 'P')
        assert.equal(describeArg.name, '')
      })

      test('execute argument', function () {
        assert.equal(executeArg.portal, '')
        assert.equal(executeArg.rows, null)
      })

      test('sync called', function () {
        assert.ok(syncCalled)
      })
    })
  })
})

var portalClient = helper.client()
var portalCon = portalClient.connection
var portalParseArg = null
portalCon.parse = function (arg) {
  portalParseArg = arg
  process.nextTick(function () {
    portalCon.emit('parseComplete')
  })
}

var portalBindArg = null
portalCon.bind = function (arg) {
  portalBindArg = arg
  process.nextTick(function () {
    portalCon.emit('bindComplete')
  })
}

var portalExecuteArg = null
portalCon.execute = function (arg) {
  portalExecuteArg = arg
  process.nextTick(function () {
    portalCon.emit('rowData', { fields: [] })
    portalCon.emit('commandComplete', { text: '' })
  })
}

var portalDescribeArg = null
portalCon.describe = function (arg) {
  portalDescribeArg = arg
  process.nextTick(function () {
    portalCon.emit('rowDescription', { fields: [] })
  })
}

portalCon.flush = function () {}
portalCon.sync = function () {
  process.nextTick(function () {
    portalCon.emit('readyForQuery')
  })
}

test('prepared statement with explicit portal', function () {
  assert.ok(portalClient.connection.emit('readyForQuery'))

  var query = portalClient.query(
    new Query({
      text: 'select * from X where name = $1',
      portal: 'myportal',
      values: ['hi'],
    })
  )

  assert.emits(query, 'end', function () {
    test('bind argument', function () {
      assert.equal(portalBindArg.portal, 'myportal')
    })

    test('describe argument', function () {
      assert.equal(portalDescribeArg.name, 'myportal')
    })

    test('execute argument', function () {
      assert.equal(portalExecuteArg.portal, 'myportal')
    })
  })
})

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
require('./test-helper')

var sasl = require('../../../lib/sasl')

test('sasl/scram', function () {
  test('startSession', function () {
    test('fails when mechanisms does not include SCRAM-SHA-256', function () {
      assert.throws(
        function () {
          sasl.startSession([])
        },
        {
          message: 'SASL: Only mechanism SCRAM-SHA-256 is currently supported',
        }
      )
    })

    test('returns expected session data', function () {
      const session = sasl.startSession(['SCRAM-SHA-256'])

      assert.equal(session.mechanism, 'SCRAM-SHA-256')
      assert.equal(String(session.clientNonce).length, 24)
      assert.equal(session.message, 'SASLInitialResponse')

      assert(session.response.match(/^n,,n=\*,r=.{24}/))
    })

    test('creates random nonces', function () {
      const session1 = sasl.startSession(['SCRAM-SHA-256'])
      const session2 = sasl.startSession(['SCRAM-SHA-256'])

      assert(session1.clientNonce != session2.clientNonce)
    })
  })

  test('continueSession', function () {
    test('fails when last session message was not SASLInitialResponse', function () {
      assert.throws(
        function () {
          sasl.continueSession({}, '', '')
        },
        {
          message: 'SASL: Last message was not SASLInitialResponse',
        }
      )
    })

    test('fails when nonce is missing in server message', function () {
      assert.throws(
        function () {
          sasl.continueSession(
            {
              message: 'SASLInitialResponse',
            },
            'bad-password',
            's=1,i=1'
          )
        },
        {
          message: 'SASL: SCRAM-SERVER-FIRST-MESSAGE: nonce missing',
        }
      )
    })

    test('fails when salt is missing in server message', function () {
      assert.throws(
        function () {
          sasl.continueSession(
            {
              message: 'SASLInitialResponse',
            },
            'bad-password',
            'r=1,i=1'
          )
        },
        {
          message: 'SASL: SCRAM-SERVER-FIRST-MESSAGE: salt missing',
        }
      )
    })

    test('fails when iteration is missing in server message', function () {
      assert.throws(
        function () {
          sasl.continueSession(
            {
              message: 'SASLInitialResponse',
            },
            'bad-password',
            'r=1,s=abcd'
          )
        },
        {
          message: 'SASL: SCRAM-SERVER-FIRST-MESSAGE: iteration missing',
        }
      )
    })

    test('fails when server nonce does not start with client nonce', function () {
      assert.throws(
        function () {
          sasl.continueSession(
            {
              message: 'SASLInitialResponse',
              clientNonce: '2',
            },
            'bad-password',
            'r=1,s=abcd,i=1'
          )
        },
        {
          message: 'SASL: SCRAM-SERVER-FIRST-MESSAGE: server nonce does not start with client nonce',
        }
      )
    })

    test('sets expected session data', function () {
      const session = {
        message: 'SASLInitialResponse',
        clientNonce: 'a',
      }

      sasl.continueSession(session, 'password', 'r=ab,s=abcd,i=1')

      assert.equal(session.message, 'SASLResponse')
      assert.equal(session.serverSignature, 'jwt97IHWFn7FEqHykPTxsoQrKGOMXJl/PJyJ1JXTBKc=')

      assert.equal(session.response, 'c=biws,r=ab,p=mU8grLfTjDrJer9ITsdHk0igMRDejG10EJPFbIBL3D0=')
    })
  })

  test('continueSession', function () {
    test('fails when last session message was not SASLResponse', function () {
      assert.throws(
        function () {
          sasl.finalizeSession({})
        },
        {
          message: 'SASL: Last message was not SASLResponse',
        }
      )
    })

    test('fails when server signature is not valid base64', function () {
      assert.throws(
        function () {
          sasl.finalizeSession(
            {
              message: 'SASLResponse',
              serverSignature: 'abcd',
            },
            'v=x1' // Purposefully invalid base64
          )
        },
        {
          message: 'SASL: SCRAM-SERVER-FINAL-MESSAGE: server signature must be base64',
        }
      )
    })

    test('fails when server signature does not match', function () {
      assert.throws(
        function () {
          sasl.finalizeSession(
            {
              message: 'SASLResponse',
              serverSignature: 'abcd',
            },
            'v=xyzq'
          )
        },
        {
          message: 'SASL: SCRAM-SERVER-FINAL-MESSAGE: server signature does not match',
        }
      )
    })

    test('does not fail when eveything is ok', function () {
      sasl.finalizeSession(
        {
          message: 'SASLResponse',
          serverSignature: 'abcd',
        },
        'v=abcd'
      )
    })
  })
})

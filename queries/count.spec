{
 "operations": [
  {
   "kind": "from",
   "id": "from0",
   "spec": {
    "bucket": "in"
   }
  },
  {
   "kind": "filter",
   "id": "filter1",
   "spec": {
    "fn": {
     "type": "FunctionExpression",
     "block": {
      "type": "FunctionBlock",
      "parameters": {
       "type": "FunctionParameters",
       "list": [
        {
         "type": "FunctionParameter",
         "key": {
          "type": "Identifier",
          "name": "r"
         }
        }
       ],
       "pipe": null
      },
      "body": {
       "type": "LogicalExpression",
       "operator": "and",
       "left": {
        "type": "BinaryExpression",
        "operator": "==",
        "left": {
         "type": "MemberExpression",
         "object": {
          "type": "IdentifierExpression",
          "name": "r"
         },
         "property": "_measurement"
        },
        "right": {
         "type": "StringLiteral",
         "value": "nginx"
        }
       },
       "right": {
        "type": "BinaryExpression",
        "operator": "==",
        "left": {
         "type": "MemberExpression",
         "object": {
          "type": "IdentifierExpression",
          "name": "r"
         },
         "property": "_field"
        },
        "right": {
         "type": "StringLiteral",
         "value": "bytes_sent"
        }
       }
      }
     }
    }
   }
  },
  {
   "kind": "group",
   "id": "group2",
   "spec": {
    "mode": "by",
    "columns": [
     "uri"
    ]
   }
  },
  {
   "kind": "window",
   "id": "window3",
   "spec": {
    "every": "10s",
    "period": "10s",
    "start": "0001-01-01T00:00:00Z",
    "round": "0s",
    "triggering": null,
    "timeColumn": "_time",
    "stopColumn": "_stop",
    "startColumn": "_start",
    "createEmpty": false
   }
  },
  {
   "kind": "count",
   "id": "count4",
   "spec": {
    "columns": [
     "_value"
    ]
   }
  },
  {
   "kind": "to",
   "id": "to5",
   "spec": {}
  }
 ],
 "edges": [
  {
   "parent": "from0",
   "child": "filter1"
  },
  {
   "parent": "filter1",
   "child": "group2"
  },
  {
   "parent": "group2",
   "child": "window3"
  },
  {
   "parent": "window3",
   "child": "count4"
  },
  {
   "parent": "count4",
   "child": "to5"
  }
 ],
 "resources": {
  "priority": "high",
  "concurrency_quota": 0,
  "memory_bytes_quota": 0
 },
 "now": "2019-02-04T15:09:39.627014Z"
}

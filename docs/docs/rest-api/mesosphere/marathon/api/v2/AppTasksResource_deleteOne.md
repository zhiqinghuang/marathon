## DELETE `/v2/apps/{app_id}/tasks/{task_id}?mesosphere.marathon.scale={true|false}`

Kill the task with ID `task_id` that belongs to the application `app_id`.

The query parameter `mesosphere.marathon.scale` is optional.  If `mesosphere.marathon.scale=true` is specified, then the application is scaled down one if the supplied `task_id` exists.  The `mesosphere.marathon.scale` parameter defaults to `false`.

### Example

**Request:**

```
DELETE /v2/apps/myApp/tasks/myApp_3-1389916890411 HTTP/1.1
Accept: application/json
Accept-Encoding: gzip, deflate, compress
Content-Length: 0
Content-Type: application/json; charset=utf-8
Host: localhost:8080
User-Agent: HTTPie/0.7.2
```

**Response:**

```
HTTP/1.1 200 OK
Content-Type: application/json
Server: Jetty(8.y.z-SNAPSHOT)
Transfer-Encoding: chunked

{
    "task": {
        "host": "mesos.vm", 
        "id": "myApp_3-1389916890411", 
        "ports": [
            31509, 
            31510
        ], 
        "stagedAt": "2014-01-17T00:01+0000", 
        "startedAt": "2014-01-17T00:01+0000"
    }
}
```

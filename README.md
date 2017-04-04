# Serverless Server Standard
Standard Version of Serverless Server 
---
Serverless Server lets you upload and run lambdas without much hassle about running and maintaining the server itself.
It communicates through REST, so it is easy to use and extend. Applications can be build on top of it.

Prerequisites:
- [Docker](https://www.docker.com/)
- Java 7 or higher


To build and run the Server:
---

Method 1: from Scratch
---
The Server runs on Spring, which needs to be imported.

additional Prerequisites:
- [Maven](https://maven.apache.org/)

1. Clone the repo: `https://github.com/serverlessserver/standard.git`
2. Import it into Intellij/Eclipse etc.
3. Import Modules with Maven, given in `pom.xml`.
4. Run.

Method 2: JAR
---
It works on Unix-like Systems. Windows should work too but no guarantee.

TODO make and link jar
1. Download JAR.
2. Run jar.

Method 3: Run in Docker
---
TODO make download link
1. Start Docker.
2. Download Serverless Server Image from Dockerhub.
3. Run.

Communicate with Server
---
The Server listens to `http://localhost:8080/`, so you can try things out.

Upload 
---
If you have a lambda, you can upload it by sending a ```POST``` request:
```
http://localhost:8080/lambdas
```
Headers:
```json
Content-Type: application/json
```
Body:
```json
{
   "name":"yourlambdaname", 
   "runtimeAttributes":{
   		"language":"Python3", 
		  "libraries":[], 
		  "code":"print(\"Hello world\")"
   }
}
```
You will receive a [JSON Webtoken](https://jwt.io), which you should keep, because you'll need this to access your uploaded lambda.

Update
---
If you want to update your lambda send a ```PUT``` to:

```
http://localhost:8080/lambdas/yourlambdaname
```
Headers:
```json
Content-Type: application/json
Authorization: Bearer tokenyougotwhenyouuploadedyourfunction
```
Body:
```json
{
   "name":"yourlambdaname", 
   "runtimeAttributes":{
   		"language":"Python3", 
		  "libraries":[], 
		  "code":"print(\"Bye world\")"
   }
}
```
You will receive a [JSON Webtoken](https://jwt.io), which you should keep, because you'll need this to access your updated lambda.
The old JWT is not valid anymore.

Show
---
If you want to check what you've uploaded, send a ```GET``` to:
```
http://localhost:8080/lambdas/yourlambdaname
```
Headers:
```json
Content-Type: application/json
Authorization: Bearer token
```
This will send you your lambda configuration.

Execute
---
If you want to execute your lambda, send ```POST``` to:
```
http://localhost:8080/lambdas/yourlambdaname/execute
```
Headers:
```json
Content-Type: application/json
Authorization: Bearer token
```
Body:
```json
{
	"times":"1",
  "parameters":[]
}
```

Subtoken
---
If you want to generate a Subtoken e.g. for other apps to use (= only execute) the lambda, you can send a ```GET``` with an expirydate to:
```http://localhost:8080/lambdas/panda/token?expiryDate=yourexpirydate```
```json
Content-Type: application/json
Authorization: Bearer token
```
Note that subtokens can only execute and not upload/etc. So keep your mastertoken (token you get when you upload or update) to yourself to maintain your function and
only give subtokens to customers.

Delete
---
If you want to delete your lambda, send a ```DEL``` to:
```
http://localhost:8080/lambdas/yourlambdaname
```
Headers:
```json
Content-Type: application/json
Authorization: Bearer token
```

More examples can be found under ```/src/testjava/edu/teco/serverless/``` and also you can check out the [documentation
and concepts](https://github.com/serverlessserver/concepts).



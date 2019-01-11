# Micro-Controllers
Micro-controllers enable tenants to manage the singularities of the objects in a flexible way. Thus, a micro-controller is a data behavior or management policy that is associated to one or more objects, and to one or more storage operations.
Micro-controllers act as object wrappers that control objects behavior. It is for this reason that we introduce the concept of **smart objects** in cloud object stores.
Micro-controllers follow the active storage approach, that is, they are executed close to the data within the storage infrastructure, but in a sandboxed environemnt by using Dockers.

A micro-controller is a piece of code (Java) that can manage the associated object. Thus, the programmability that micro-controllers provide allows tenants to create advanced forms of object control. There are two different types of micro-controllers: **synchronous** and **asynchronous**. A synchronous micro-controller blocks the incoming or outcoming request until it completes the execution. In the synchronous model, micro-controllers are executed in real-time, which means that the lifecycle of objects is intercepted. In contrast, asynchronous micro-controllers are event-driven, that is, they run when an event occurs. The request that triggered the event does not wait for their completion.

## Framework
This implementation of micro-controllers is designed for OpenStack Swift. This framework includes an interception middleware for Swift, and an execution engine that puts into execution the micro-controllers in a sand-boxed environment.

## Installation
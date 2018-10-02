# go-push-components

[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

> Push Components replace the need for golang chan and select constructs, which require no polling and are instead designed to &#34;push&#34; new work to the client when available.

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [API](#api)
- [Maintainers](#maintainers)
- [Contribute](#contribute)
- [License](#license)

## Background

A common golang idiom when a component needs to receive messages from another component is to poll a `chan` using a `select` loop. In this construct, the message sender publishes to the `chan` and the receiver pulls from it. `chan` is a bare-bones component. It does not manage concurrency and it requires continuous CPU work by the receiver for the polling the `chan`.

Push Components decouple the sender and receiver, as `chan` does, but it also helpfully manages other aspects of such an exchange pattern:
* concurrency
* overload
* drain, and graceful shutdown
* counting items in the component
* emptying the component

Push Components provided in this package:
* PushQueue
* PushStack

## Install

```
go get -u github.com/blocktop/go-push-components
```

## Usage

Create the push component (in this case a queue), tell it concurrency and capacity attributes, and provide a worker function.
```go
q := push.NewPushQueue(2, 50, worker)
q.Start()
   
func worker(item Item) {
  doWorkWithItem(item)
}
```


## API

## Maintainers

[@strobus](https://github.com/strobus)

## Contribute

PRs accepted.

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT © 2018 J. Strobus White

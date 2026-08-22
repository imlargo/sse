// Package compat holds compatibility tests against third-party HTTP
// frameworks.
//
// It is a separate module on purpose. Its dependencies are Gin and Echo, and
// keeping them out of the root module's graph is what lets RF-H1 be checked
// mechanically: using this library with the standard library must not drag in a
// framework.
//
// There is no code here, only tests. That is the finding: Gin and Echo need no
// adapter. Both wrap http.ResponseWriter and both implement
// Unwrap() http.ResponseWriter, so http.ResponseController reaches the real
// writer and flushing and write deadlines work through them untouched.
//
// Shipping adapters for them would be code with no purpose, and code with no
// purpose still has to be maintained. What is worth having instead is a test
// that fails the day one of them stops unwrapping.
package compat

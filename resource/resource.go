package resource

import (
	"reflect"
	"regexp"
	"strings"
)

// IVersionedResource is an interface that represents a resource that has a version.
type IVersionedResource interface {
	GetResourceVersion() uint64
	SetResourceVersion(uint64)
}

// Meta is a struct that contains metadata for a resource.
type Meta struct {
	ResourceVersion uint64 `gorm:"index"`
}

// GetResourceVersion returns the resource version.
func (m *Meta) GetResourceVersion() uint64 {
	return m.ResourceVersion
}

// SetResourceVersion sets the resource version.
func (m *Meta) SetResourceVersion(version uint64) {
	m.ResourceVersion = version
}

// toSnakeCase converts a string to snake case.
func toSnakeCase(str string) string {
	matchUpperCase := regexp.MustCompile("([a-z0-9])([A-Z])")
	snake := matchUpperCase.ReplaceAllString(str, "${1}_${2}")
	return strings.ToLower(snake)
}

// GetResourceName returns snake case name of the resource.
func GetResourceName(resource IVersionedResource) string {
	t := reflect.TypeOf(resource)

	// 如果传入的是指针，获取指针指向的元素类型
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// 获取类型名称并转换为snake case
	return toSnakeCase(t.Name())
}

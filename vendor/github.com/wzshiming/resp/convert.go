package resp

import (
	"fmt"
	"reflect"
	"strconv"
	"unsafe"
)

func ConvertFrom(r Reply, i interface{}) error {
	val := reflect.ValueOf(i)
	return convertFrom(r, val)
}

func convertFrom(r Reply, val reflect.Value) error {
	switch val.Kind() {
	default:
		return fmt.Errorf("Error Data types are not supported '%s'", val.Kind().String())
	case reflect.Bool:
		switch b := r.(type) {
		default:
			return fmt.Errorf("Error Not a valid Integer value '%s'", b.Format(0))
		case ReplyBulk:
			v := *(*string)(unsafe.Pointer(&b))
			bb, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			val.SetBool(bb)
			return nil
		case ReplyInteger:
			v := *(*string)(unsafe.Pointer(&b))
			bb, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			val.SetBool(bb)
			return nil
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch b := r.(type) {
		default:
			return fmt.Errorf("Error Not a valid Integer value '%s'", b.Format(0))
		case ReplyBulk:
			v := *(*string)(unsafe.Pointer(&b))
			bb, err := strconv.ParseInt(v, 0, 0)
			if err != nil {
				return err
			}
			val.SetInt(bb)
			return nil
		case ReplyInteger:
			v := *(*string)(unsafe.Pointer(&b))
			bb, err := strconv.ParseInt(v, 0, 0)
			if err != nil {
				return err
			}
			val.SetInt(bb)
			return nil
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		switch b := r.(type) {
		default:
			return fmt.Errorf("Error Not a valid Integer value '%s'", b.Format(0))
		case ReplyBulk:
			v := *(*string)(unsafe.Pointer(&b))
			bb, err := strconv.ParseUint(v, 0, 0)
			if err != nil {
				return err
			}
			val.SetUint(bb)
			return nil
		case ReplyInteger:
			v := *(*string)(unsafe.Pointer(&b))
			bb, err := strconv.ParseUint(v, 0, 0)
			if err != nil {
				return err
			}
			val.SetUint(bb)
			return nil
		}

	case reflect.Interface:
		if val.IsNil() {
			return nil
		}
		return convertFrom(r, val.Elem())

	case reflect.Ptr:
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		return convertFrom(r, val.Elem())

	case reflect.Map:
		switch b := r.(type) {
		default:
			return fmt.Errorf("Error Not a valid MultiBulk value '%s'", b.Format(0))
		case ReplyMultiBulk:
			if len(b)%2 != 0 {
				return fmt.Errorf("Error Not a pair")
			}

			typ := val.Type()
			if val.IsNil() {
				val.Set(reflect.MakeMap(typ))
			}
			typKey := typ.Key()
			typValue := typ.Elem()
			for i := 0; i != len(b); i += 2 {
				key := reflect.New(typKey)
				err := convertFrom(b[i], key)
				if err != nil {
					return err
				}
				value := reflect.New(typValue)
				err = convertFrom(b[i+1], value)
				if err != nil {
					return err
				}
				val.SetMapIndex(key.Elem(), value.Elem())
			}
			return nil
		}

	case reflect.Struct:
		switch b := r.(type) {
		default:
			return fmt.Errorf("Error Not a valid MultiBulk value '%s'", b.Format(0))
		case ReplyMultiBulk:
			if len(b)%2 != 0 {
				return fmt.Errorf("Error Not a pair")
			}
			var key string
			rkey := reflect.ValueOf(&key).Elem()
			for i := 0; i != len(b); i += 2 {
				err := convertFrom(b[i], rkey)
				if err != nil {
					return err
				}
				field := val.FieldByName(key)
				value := reflect.New(field.Type())
				err = convertFrom(b[i+1], value)
				if err != nil {
					return err
				}
				field.Set(value.Elem())
			}
			return nil
		}

	case reflect.Slice:
		switch b := r.(type) {
		default:
			return fmt.Errorf("Error Not a valid MultiBulk value '%s'", b.Format(0))
		case ReplyBulk:
			if val.Type().Elem().Kind() == reflect.Uint8 {
				val.SetBytes([]byte(b))
				return nil
			}
			return fmt.Errorf("Error Not a valid MultiBulk value '%s'", b.Format(0))
		case ReplyInteger:
			if val.Type().Elem().Kind() == reflect.Uint8 {
				val.SetBytes([]byte(b))
				return nil
			}
			return fmt.Errorf("Error Not a valid MultiBulk value '%s'", b.Format(0))
		case ReplyMultiBulk:
			cap := val.Cap()
			if cap < len(b) {
				val.Set(reflect.MakeSlice(val.Type(), len(b), len(b)))
			} else {
				val.SetLen(len(b))
			}
			for i, v := range b {
				err := convertFrom(v, val.Index(i))
				if err != nil {
					return err
				}
			}
			return nil
		}

	case reflect.Array:
		switch b := r.(type) {
		default:
			return fmt.Errorf("Error Not a valid MultiBulk value '%s'", b.Format(0))
		case ReplyMultiBulk:
			cap := val.Cap()
			if len(b) > cap {
				return fmt.Errorf("Error Greater than array cap")
			}
			for i, v := range b {
				err := convertFrom(v, val.Index(i))
				if err != nil {
					return err
				}
			}
			return nil
		}
	case reflect.String:
		switch b := r.(type) {
		default:
			return fmt.Errorf("Error Not a valid Bulk value '%s'", b.Format(0))
		case ReplyBulk:
			v := *(*string)(unsafe.Pointer(&b))
			val.SetString(v)
			return nil
		case ReplyInteger:
			v := *(*string)(unsafe.Pointer(&b))
			val.SetString(v)
			return nil
		}
	}
}

// ConvertTo returns convert the base type to Reply
func ConvertTo(i interface{}) (Reply, error) {
	val := reflect.ValueOf(i)
	return convertTo(val)
}

func convertTo(val reflect.Value) (Reply, error) {
	switch val.Kind() {
	default:
		return nil, fmt.Errorf("Error Data types are not supported '%s'", val.Kind().String())
	case reflect.Bool:
		if val.Bool() {
			return ReplyInteger{'1'}, nil
		}
		return ReplyInteger{'0'}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return ReplyInteger(strconv.AppendInt(nil, val.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return ReplyInteger(strconv.AppendUint(nil, val.Uint(), 10)), nil
	case reflect.Interface, reflect.Ptr:
		return convertTo(val.Elem())
	case reflect.Map:
		num := val.Len()
		items := make(ReplyMultiBulk, 0, num*val.Len())

		for _, k := range val.MapKeys() {
			item, err := convertTo(k)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
			item, err = convertTo(val.MapIndex(k))
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return items, nil
	case reflect.Struct:
		num := val.NumField()
		typ := val.Type()
		items := make(ReplyMultiBulk, 0, num*2)
		for i := 0; i != num; i++ {
			name := typ.Field(i).Name
			items = append(items, ReplyBulk(*(*[]byte)(unsafe.Pointer(&name))))
			item, err := convertTo(val.Field(i))
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return items, nil
	case reflect.Slice:
		switch val.Type().Elem().Kind() {
		case reflect.Uint8:
			return ReplyBulk(val.Bytes()), nil
		}
		num := val.Len()
		items := make(ReplyMultiBulk, 0, num)
		for i := 0; i != num; i++ {
			item, err := convertTo(val.Index(i))
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return items, nil
	case reflect.Array:
		num := val.Len()
		items := make(ReplyMultiBulk, 0, num)
		for i := 0; i != num; i++ {
			item, err := convertTo(val.Index(i))
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return items, nil
	case reflect.String:
		com := val.String()
		return ReplyBulk(*(*[]byte)(unsafe.Pointer(&com))), nil
	}

}

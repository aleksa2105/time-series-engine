package entry

type Entry interface {
	Serialize() []byte
	Size() uint64
	GetValue() uint64
}

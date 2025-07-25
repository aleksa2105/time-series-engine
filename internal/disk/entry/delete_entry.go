package entry

/*
	'0' For not deleted
	'1' For deleted
*/

const ActiveBit uint8 = 0
const DeletedBit uint8 = 1

type DeleteEntry struct {
	Deleted bool
}

func NewDeleteEntry(deleted bool) *DeleteEntry {
	return &DeleteEntry{
		Deleted: deleted,
	}
}

func (de *DeleteEntry) Serialize() []byte {
	allBytes := make([]byte, 1)
	if de.Deleted == true {
		allBytes[0] = DeletedBit
	} else {
		allBytes[0] = ActiveBit
	}
	return allBytes
}

func (de *DeleteEntry) Size() uint64 {
	return 1
}

func (de *DeleteEntry) GetValue() uint64 {
	if de.Deleted {
		return uint64(DeletedBit)
	}
	return uint64(ActiveBit)
}

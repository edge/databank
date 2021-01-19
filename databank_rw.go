package databank

func (d *databank) ReadInt16(id string) (int16, bool) {
	if e, ok := d.Read(id); ok {
		return e.ReadInt16(), ok
	}
	return 0, false
}

func (d *databank) ReadInt32(id string) (int32, bool) {
	if e, ok := d.Read(id); ok {
		return e.ReadInt32(), true
	}
	return 0, false
}

func (d *databank) ReadInt64(id string) (int64, bool) {
	if e, ok := d.Read(id); ok {
		return e.ReadInt64(), true
	}
	return 0, false
}

func (d *databank) ReadString(id string) (string, bool) {
	if e, ok := d.Read(id); ok {
		return e.ReadString(), true
	}
	return "", false
}

func (d *databank) ReadUint16(id string) (uint16, bool) {
	if e, ok := d.Read(id); ok {
		return e.ReadUint16(), true
	}
	return 0, false
}

func (d *databank) ReadUint32(id string) (uint32, bool) {
	if e, ok := d.Read(id); ok {
		return e.ReadUint32(), true
	}
	return 0, false
}

func (d *databank) ReadUint64(id string) (uint64, bool) {
	if e, ok := d.Read(id); ok {
		return e.ReadUint64(), true
	}
	return 0, false
}

func (d *databank) WriteInt16(key string, val int16) (*Entry, bool) {
	e := d.NewEntry(key)
	e.WriteInt16(val)
	return e, d.Write(e)
}

func (d *databank) WriteInt32(key string, val int32) (*Entry, bool) {
	e := d.NewEntry(key)
	e.WriteInt32(val)
	return e, d.Write(e)
}

func (d *databank) WriteInt64(key string, val int64) (*Entry, bool) {
	e := d.NewEntry(key)
	e.WriteInt64(val)
	return e, d.Write(e)
}

func (d *databank) WriteString(key, val string) (*Entry, bool) {
	e := d.NewEntry(key)
	e.WriteString(val)
	return e, d.Write(e)
}

func (d *databank) WriteUint16(key string, val uint16) (*Entry, bool) {
	e := d.NewEntry(key)
	e.WriteUint16(val)
	return e, d.Write(e)
}

func (d *databank) WriteUint32(key string, val uint32) (*Entry, bool) {
	e := d.NewEntry(key)
	e.WriteUint32(val)
	return e, d.Write(e)
}

func (d *databank) WriteUint64(key string, val uint64) (*Entry, bool) {
	e := d.NewEntry(key)
	e.WriteUint64(val)
	return e, d.Write(e)
}

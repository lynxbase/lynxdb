package segment

// IndexProfile returns the catalog index profile for the named column.
func (r *Reader) IndexProfile(name string) IndexProfile {
	for _, cat := range r.footer.Catalog {
		if cat.Name == name {
			return cat.IndexProfile
		}
	}

	return IndexProfileDefault
}

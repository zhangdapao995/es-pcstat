package es_collect

import (
	mapset "github.com/deckarep/golang-set"
)

var filteredSuffixSet = mapset.NewSet("lock", "SEGMENT_N", "si", "cfe", "liv", "dvm", "nvm", "dii", "fdx", "fnm")

//use it for filter .lock .si file,which use little page cache
func fileSuffixFilter(files []string) []string {
	resultFiles := make([]string, 0)
	for _, file := range files {
		exist := filteredSuffixSet.Contains(getFileSuffix(file))
		if !exist {
			resultFiles = append(resultFiles, file)
		}
	}

	return resultFiles
}

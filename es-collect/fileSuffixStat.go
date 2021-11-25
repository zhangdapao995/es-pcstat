package es_collect

type FileSuffixStat map[string]FileSuffixCache

func (fileSuffixStat FileSuffixStat) Add(suffixName string, pageCache int, primary bool) {
	fileSubffixCache, exist := (fileSuffixStat)[suffixName]
	priPageCache := 0
	repPageCache := 0
	totalPageCache := 0
	if exist {
		totalPageCache = fileSubffixCache.pageCache
		priPageCache = fileSubffixCache.priPageCache
		repPageCache = fileSubffixCache.repPageCache
	}
	if primary {
		priPageCache += pageCache
	} else {
		repPageCache += pageCache
	}
	totalPageCache += pageCache
	(fileSuffixStat)[suffixName] = FileSuffixCache{suffixName: suffixName, pageCache: totalPageCache,
		priPageCache: priPageCache, repPageCache: repPageCache}
}

func (fileSuffixStat FileSuffixStat) AddAll(fileSuffixStatTo FileSuffixStat, primary bool) {
	for _, fileSuffixCache := range fileSuffixStatTo {
		fileSuffixStat.Add(fileSuffixCache.suffixName, fileSuffixCache.pageCache, primary)
	}
}

type FileSuffixCache struct {
	suffixName   string
	pageCache    int
	priPageCache int
	repPageCache int
}

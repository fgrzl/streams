package models

import (
	"sort"
)

// TieredPage represents a page with tiering information.
type TieredPage struct {
	Tier int32
	Page *Page
}

// CoveringPages represents the result of a coverage calculation.
type CoveringPages struct {
	Pages   []TieredPage
	Covered bool
}

const (
	PageSizeZero    int64 = 0
	PageSizeNano    int64 = 1024 * 1024 * 4
	PageSizeMicro   int64 = PageSizeNano * 4
	PageSizeSmall   int64 = PageSizeMicro * 8
	PageSizeMedium  int64 = PageSizeSmall * 10
	PageSizeLarge   int64 = PageSizeMedium * 12
	PageSizeXLarge  int64 = PageSizeLarge * 14
	PageSize2XLarge int64 = PageSizeXLarge * 16
)

var (
	minPageSizes = map[int32]int64{
		0: PageSizeZero,
		1: PageSizeNano,
		2: PageSizeMicro,
		3: PageSizeSmall,
		4: PageSizeMedium,
		5: PageSizeLarge,
		6: PageSizeXLarge,
	}

	maxPageSizes = map[int32]int64{
		0: PageSizeNano,
		1: PageSizeMicro,
		2: PageSizeSmall,
		3: PageSizeMedium,
		4: PageSizeLarge,
		5: PageSizeXLarge,
		6: PageSize2XLarge,
	}
)

// EmptyPage is the default empty page
var EmptyPage = &Page{}

// GetMinPageSize returns the minimum page size for the given storage tier
func GetMinPageSize(tier int32) int64 {
	return minPageSizes[tier]
}

// GetMaxPageSize returns the maximum page size for the given storage tier
func GetMaxPageSize(tier int32) int64 {
	return maxPageSizes[tier]
}

// GetCoveringPages calculates the pages that cover the given sequence range.
func GetCoveringPages(pages []TieredPage, minSequence, maxSequence uint64) *CoveringPages {
	// minSequence can never be less than 1
	if minSequence < 1 {
		minSequence = 1
	}

	// Filter and sort the pages
	sortedPages := make([]TieredPage, 0)
	for _, page := range pages {
		if page.Page.LastSequence > minSequence {
			sortedPages = append(sortedPages, page)
		}
	}

	sort.Slice(sortedPages, func(i, j int) bool {
		if sortedPages[i].Page.FirstSequence == sortedPages[j].Page.FirstSequence {
			return sortedPages[i].Page.LastSequence > sortedPages[j].Page.LastSequence
		}
		return sortedPages[i].Page.FirstSequence < sortedPages[j].Page.FirstSequence
	})

	// Determine the pages that best cover the request
	coveringPages := make([]TieredPage, 0, len(sortedPages))
	var nextFirstSequence uint64

	for _, sortedPage := range sortedPages {
		if len(coveringPages) == 0 {
			coveringPages = append(coveringPages, sortedPage)
			nextFirstSequence = sortedPage.Page.FirstSequence
			continue
		}

		lastCoveringPage := coveringPages[len(coveringPages)-1]

		if lastCoveringPage.Page.FirstSequence == sortedPage.Page.FirstSequence ||
			lastCoveringPage.Page.LastSequence >= sortedPage.Page.LastSequence {
			continue
		}

		// Check if the current page overlaps with the last added covering page
		if lastCoveringPage.Page.LastSequence <= sortedPage.Page.LastSequence &&
			sortedPage.Page.FirstSequence-1 <= nextFirstSequence {
			coveringPages[len(coveringPages)-1] = sortedPage
			nextFirstSequence = max(lastCoveringPage.Page.LastSequence, sortedPage.Page.FirstSequence)
			continue
		}

		coveringPages = append(coveringPages, sortedPage)
		nextFirstSequence = max(lastCoveringPage.Page.LastSequence, sortedPage.Page.FirstSequence)
	}

	if len(coveringPages) == 0 {
		return &CoveringPages{Pages: coveringPages, Covered: false}
	}

	covered := coveringPages[0].Page.FirstSequence <= minSequence &&
		coveringPages[len(coveringPages)-1].Page.LastSequence >= maxSequence
	return &CoveringPages{Pages: coveringPages, Covered: covered}
}

// max returns the maximum of two int64 values.
func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// Find the nearest key without exceeding the target
func (page *Page) FindNearestKey(sequence uint64) (uint64, int64) {
	ix := page.Index
	ixLen := len(ix)
	if ixLen == 0 {
		return 0, 0
	}

	pos := sort.Search(ixLen, func(i int) bool {
		return ix[i].Sequence > sequence
	})

	if pos == 0 {
		return 0, 0
	}

	// Return the largest key <= target
	entry := ix[pos-1]
	return entry.Sequence, entry.Position
}

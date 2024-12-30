package models_test

import (
	"testing"

	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/pkg/util"
	"github.com/stretchr/testify/assert"
)

var T0 = int32(0)
var T1 = int32(1)
var T2 = int32(2)

var (
	timestamp = util.GetTimestamp()
	pages     = []models.TieredPage{
		/* 0  */ {Tier: T0, Page: &models.Page{Number: 1, FirstSequence: 1, FirstTimestamp: timestamp, LastSequence: 10, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 1  */ {Tier: T0, Page: &models.Page{Number: 2, FirstSequence: 11, FirstTimestamp: timestamp, LastSequence: 20, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 2  */ {Tier: T0, Page: &models.Page{Number: 3, FirstSequence: 21, FirstTimestamp: timestamp, LastSequence: 30, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 3  */ {Tier: T0, Page: &models.Page{Number: 4, FirstSequence: 31, FirstTimestamp: timestamp, LastSequence: 40, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 4  */ {Tier: T0, Page: &models.Page{Number: 5, FirstSequence: 41, FirstTimestamp: timestamp, LastSequence: 50, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 5  */ {Tier: T0, Page: &models.Page{Number: 6, FirstSequence: 51, FirstTimestamp: timestamp, LastSequence: 60, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 6  */ {Tier: T0, Page: &models.Page{Number: 7, FirstSequence: 61, FirstTimestamp: timestamp, LastSequence: 70, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 7  */ {Tier: T0, Page: &models.Page{Number: 8, FirstSequence: 71, FirstTimestamp: timestamp, LastSequence: 80, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 8  */ {Tier: T0, Page: &models.Page{Number: 9, FirstSequence: 81, FirstTimestamp: timestamp, LastSequence: 90, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 9  */ {Tier: T0, Page: &models.Page{Number: 10, FirstSequence: 91, FirstTimestamp: timestamp, LastSequence: 100, LastTimestamp: timestamp, Count: 10, Size: 999}},
		/* 10 */ {Tier: T1, Page: &models.Page{Number: 1, FirstSequence: 1, FirstTimestamp: timestamp, LastSequence: 20, LastTimestamp: timestamp, Count: 20, Size: 999}},
		/* 11 */ {Tier: T1, Page: &models.Page{Number: 2, FirstSequence: 21, FirstTimestamp: timestamp, LastSequence: 40, LastTimestamp: timestamp, Count: 20, Size: 999}},
		/* 12 */ {Tier: T1, Page: &models.Page{Number: 3, FirstSequence: 41, FirstTimestamp: timestamp, LastSequence: 60, LastTimestamp: timestamp, Count: 20, Size: 999}},
		/* 13 */ {Tier: T1, Page: &models.Page{Number: 4, FirstSequence: 71, FirstTimestamp: timestamp, LastSequence: 90, LastTimestamp: timestamp, Count: 20, Size: 999}},
		/* 14 */ {Tier: T2, Page: &models.Page{Number: 1, FirstSequence: 1, FirstTimestamp: timestamp, LastSequence: 25, LastTimestamp: timestamp, Count: 25, Size: 999}},
		/* 15 */ {Tier: T2, Page: &models.Page{Number: 2, FirstSequence: 21, FirstTimestamp: timestamp, LastSequence: 50, LastTimestamp: timestamp, Count: 25, Size: 999}},
		/* 16 */ {Tier: T2, Page: &models.Page{Number: 2, FirstSequence: 51, FirstTimestamp: timestamp, LastSequence: 75, LastTimestamp: timestamp, Count: 25, Size: 999}},
	}
)

func TestShouldReturnCoveringPagesAndIndicateNoAdditionalPagesAreNeeded(t *testing.T) {
	// Arrange
	var tieredPages []models.TieredPage
	for _, p := range pages {
		if p.Tier == T0 {
			tieredPages = append(tieredPages, p)
		}
	}

	// Act
	coveringPages := models.GetCoveringPages(tieredPages, 1, 100)

	// Assert
	assert.True(t, coveringPages.Covered)
	assert.ElementsMatch(t, tieredPages, coveringPages.Pages)
}

func TestShouldReturnCoveringPagesAndIndicateAdditionalPagesAreNeeded(t *testing.T) {
	// Arrange
	var tieredPages []models.TieredPage
	for _, p := range pages {
		if p.Tier == T0 && p.Page.LastSequence > 50 {
			tieredPages = append(tieredPages, p)
		}
	}

	// Act
	coveringPages := models.GetCoveringPages(tieredPages, 1, 100)

	// Assert
	assert.False(t, coveringPages.Covered)
	assert.ElementsMatch(t, tieredPages, coveringPages.Pages)
}

func TestShouldReturnFewestNumberOfPagesCoveringTheRange(t *testing.T) {
	// Arrange
	tieredPages := pages

	// Act
	coveringPages := models.GetCoveringPages(tieredPages, 1, 100)

	// Assert
	assert.True(t, coveringPages.Covered)

	optimalSet := []models.TieredPage{
		tieredPages[14],
		tieredPages[15],
		tieredPages[16],
		tieredPages[13],
		tieredPages[9],
	}

	assert.ElementsMatch(t, optimalSet, coveringPages.Pages)
}

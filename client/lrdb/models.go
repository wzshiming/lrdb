package lrdb

type Info struct {
	WriteDelayCount    int
	WriteDelayDuration int
	WritePaused        int
	AliveSnapshots     int
	AliveIterators     int
	IOWrite            int
	IORead             int
	BlockCacheSize     int
	OpenedTablesCount  int
	LevelSizes         []int
	LevelTablesCounts  []int
	LevelRead          []int
	LevelWrite         []int
	LevelDurations     []int
}

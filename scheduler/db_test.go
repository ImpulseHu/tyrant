package scheduler

import (
	"os"
	"testing"
)

func init() {
	InitConfig("../config.ini")
}

func TestDbMap(t *testing.T) {
	InitSharedDbMap()
	defer func() {
		os.Remove("/tmp/test.db")
	}()
	j := &Job{
		Name: "Test",
	}

	j2 := Job{}

	err := sharedDbMap.Insert(j)
	if err != nil {
		t.Error(err)
	}

	err = sharedDbMap.SelectOne(&j2, "select * from jobs where name = ?", j.Name)
	if err != nil {
		t.Error(err)
	}
}

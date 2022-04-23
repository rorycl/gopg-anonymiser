package main

import (
	"testing"
)

func TestLoadFilters(t *testing.T) {

	settings := Settings{
		"a": []Filter{
			Filter{
				Filter:       "string replace",
				Columns:      []string{"a", "c"},
				Replacements: []string{"abc", "def"},
			},
		},
		"b": []Filter{
			Filter{
				Filter:  "uuid",
				Columns: []string{"b", "d"},
			},
		},
	}

	tableFilters, err := loadFilters(settings)
	if err != nil {
		t.Errorf("load filter error %s", err)
	}
	if len(tableFilters) != 2 {
		t.Errorf("length of tableFilters should be 2, is %d", len(tableFilters))
	}
	if tableFilters["b"][0].FilterName() != "uuid replace" {
		t.Errorf("filter name not uuid replace, got %s", tableFilters["b"][0].FilterName())
	}
	t.Logf("tableFilters: %T %+v\n", tableFilters, tableFilters)

}

func TestLoadFiltersFail(t *testing.T) {

	// all tests should fail
	tests := []struct {
		name    string
		setting Settings
	}{
		{
			name: "string replace should fail with no columns",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:       "string replace",
						Columns:      []string{},
						Replacements: []string{"abc", "def"},
					},
				},
			},
		},
		{
			name: "string replace should fail with col len != replacement len",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:       "string replace",
						Columns:      []string{"a", "c", "d"},
						Replacements: []string{"abc", "def"},
					},
				},
			},
		},
		{
			name: "uuid replace should fail with no columns",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:  "uuid",
						Columns: []string{},
					},
				},
			},
		},
		{
			name: "file replace should fail with no columns",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:  "file replace",
						Columns: []string{},
						Source:  "/dev/random",
					},
				},
			},
		},
		{
			name: "file replace should fail with no source",
			setting: Settings{
				"b": []Filter{
					Filter{
						Filter:  "file replace",
						Columns: []string{"a", "c"},
						Source:  "",
					},
				},
			},
		},
	}

	for _, tc := range tests {
		_, err := loadFilters(tc.setting)
		if err == nil {
			t.Errorf("test %s failed: %s", tc.name, err)
		}
		t.Log(err)
	}
}

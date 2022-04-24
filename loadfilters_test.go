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

	tf, err := loadFilters(settings)
	if err != nil {
		t.Errorf("load filter error %s", err)
	}
	if len(tf.tableFilters) != 2 {
		t.Errorf("length of tableFilters should be 2, is %d", len(tf.tableFilters))
	}
	if tf.tableFilters["b"][0].FilterName() != "uuid replace" {
		t.Errorf("filter name not uuid replace, got %s", tf.tableFilters["b"][0].FilterName())
	}
	t.Logf("tableFilters: %T %+v\n", tf, tf)

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

func TestLoadFiltersFailDelete(t *testing.T) {

	settings := Settings{
		"a": []Filter{
			Filter{
				Filter:       "string replace",
				Columns:      []string{"a", "c"},
				Replacements: []string{"abc", "def"},
			},
			Filter{
				Filter:       "delete",
				Columns:      []string{},
				Replacements: []string{},
			},
		},
	}

	_, err := loadFilters(settings)
	if err == nil {
		t.Errorf("load filters should fail for more than one delete filter")
	}
	t.Log(err)
}

func TestLoadFiltersFailCircularRefs(t *testing.T) {

	settings := Settings{
		"public.a": []Filter{
			Filter{
				Filter:       "string replace",
				Columns:      []string{"a", "c"},
				Replacements: []string{"abc", "def"},
			},
			Filter{
				Filter:       "reference replace",
				Columns:      []string{"a"},
				Replacements: []string{"b"},
				OptArgs: map[string][2]string{
					"fklookup": [2]string{
						"public.b.a", "c",
					},
				},
			},
		},
		"public.b": []Filter{
			Filter{
				Filter:       "string replace",
				Columns:      []string{"a", "c"},
				Replacements: []string{"abc", "def"},
			},
			Filter{
				Filter:       "reference replace",
				Columns:      []string{"a"},
				Replacements: []string{"b"},
				OptArgs: map[string][2]string{
					"fklookup": [2]string{
						"public.a.a", "c",
					},
				},
			},
		},
	}

	_, err := loadFilters(settings)
	if err == nil {
		t.Errorf("load filters should detect circular reference")
	}
	t.Log(err)
}

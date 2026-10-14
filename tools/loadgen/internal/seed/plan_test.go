package seed

import (
	"testing"
	"time"
)

func validSpec() Spec {
	return Spec{Users: 4, Auctions: 10, AuctionDuration: 7 * 24 * time.Hour, CurrencyCode: "USD"}
}

func TestBuildPlanRotatesOwnersAndIsDeterministic(t *testing.T) {
	first, err := BuildPlan(validSpec())
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildPlan(validSpec())
	if err != nil {
		t.Fatal(err)
	}

	if len(first) != 10 {
		t.Fatalf("planned %d auctions, want 10", len(first))
	}
	for i, request := range first {
		if request.OwnerIndex != i%4 {
			t.Errorf("auction %d owned by user %d, want %d", i, request.OwnerIndex, i%4)
		}
		if request != second[i] {
			t.Errorf("auction %d differs between two builds of one spec", i)
		}
		if request.Duration != 7*24*time.Hour || request.CurrencyCode != "USD" {
			t.Errorf("auction %d lost the spec's duration or currency: %+v", i, request)
		}
	}
	if first[0].Title == first[1].Title {
		t.Error("titles are not unique")
	}
}

func TestBuildPlanRejectsInvalidSpecs(t *testing.T) {
	cases := map[string]func(*Spec){
		"one user":         func(s *Spec) { s.Users = 1 },
		"negative count":   func(s *Spec) { s.Auctions = -1 },
		"duration too low": func(s *Spec) { s.AuctionDuration = time.Minute },
		"duration too big": func(s *Spec) { s.AuctionDuration = 30 * 24 * time.Hour },
		"bad currency":     func(s *Spec) { s.CurrencyCode = "US" },
	}

	for name, mutate := range cases {
		spec := validSpec()
		mutate(&spec)
		_, err := BuildPlan(spec)
		if err == nil {
			t.Errorf("%s: spec was accepted", name)
		}
	}
}

func TestBuildPlanAllowsZeroAuctions(t *testing.T) {
	spec := validSpec()
	spec.Auctions = 0
	plan, err := BuildPlan(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 0 {
		t.Fatalf("planned %d auctions, want none", len(plan))
	}
}

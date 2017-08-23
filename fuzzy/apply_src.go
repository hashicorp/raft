package fuzzy

import (
	"hash/fnv"
	"math/rand"
	"testing"
	"time"
)

type applySource struct {
	rnd  *rand.Rand
	seed int64
}

// newApplySource will create a new source, any source created with the same seed will generate the same sequence of data.
func newApplySource(seed string) *applySource {
	h := fnv.New32()
	h.Write([]byte(seed))
	s := &applySource{seed: int64(h.Sum32())}
	s.reset()
	return s
}

// reset this source back to its initial state, it'll generate the same sequence of data it initally did
func (a *applySource) reset() {
	a.rnd = rand.New(rand.NewSource(a.seed))
}

func (a *applySource) nextEntry() []byte {
	const sz = 33
	r := make([]byte, sz)
	for i := 0; i < len(r); i++ {
		r[i] = byte(a.rnd.Int31n(256))
	}
	return r
}

type clusterApplier struct {
	stopCh  chan bool
	applied uint64
	src     *applySource
}

// runs apply in chunks of n to the cluster, use the returned Applier to Stop() it
func (a *applySource) apply(t *testing.T, c *cluster, n uint) *clusterApplier {
	ap := &clusterApplier{stopCh: make(chan bool), src: a}
	go ap.apply(t, c, n)
	return ap
}

func (ca *clusterApplier) apply(t *testing.T, c *cluster, n uint) {
	for true {
		select {
		case <-ca.stopCh:
			return
		default:
			ca.applied += c.ApplyN(t, time.Second, ca.src, n)
		}
	}
}

func (ca *clusterApplier) stop() {
	ca.stopCh <- true
	close(ca.stopCh)
}

package web

import (
	"context"
	"net/http"
	"tgragnato.it/magnetico/types/infohash"
	infohash_v2 "tgragnato.it/magnetico/types/infohash-v2"
)

func infohashMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		infohashHex := r.PathValue("infohash")

		var infohashBytes []byte
		if h1 := infohash.FromHexString(infohashHex); !h1.IsZero() {
			infohashBytes = h1.Bytes()
		} else if h2 := infohash_v2.FromHexString(infohashHex); !h2.IsZero() {
			infohashBytes = h2.Bytes()
		} else {
			http.Error(w, "Couldn't decode infohash", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		ctx = context.WithValue(ctx, InfohashKey, infohashBytes)
		next.ServeHTTP(w, r.WithContext(ctx))
	}
}

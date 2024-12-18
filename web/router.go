package web

import (
	"embed"
	"go.uber.org/zap"
	"net/http"
	"time"

	"tgragnato.it/magnetico/persistence"
	"tgragnato.it/magnetico/stats"
)

var (
	//go:embed static/**
	static   embed.FS
	database persistence.Database
)

type InfohashKeyType string

const (
	ContentType     string          = "Content-Type"
	ContentTypeJson string          = "application/json; charset=utf-8"
	InfohashKey     InfohashKeyType = "infohash"
)

func StartWeb(address string, cred map[string][]byte, db persistence.Database) {
	credentials = cred
	database = db
	zap.L().Info("web",
		zap.String("info", "magnetico is ready to serve on "+address+" !"))
	server := &http.Server{
		Addr:         address,
		Handler:      makeRouter(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil {
		zap.L().Fatal("web",
			zap.String("info", "ListenAndServe error"),
			zap.Error(err))
	}
}

func makeRouter() *http.ServeMux {
	router := http.NewServeMux()

	router.HandleFunc("/", BasicAuth(rootHandler))

	staticFS := http.FS(static)
	router.HandleFunc("/static/", BasicAuth(
		http.StripPrefix("/", http.FileServer(staticFS)).ServeHTTP,
	))

	router.HandleFunc("/metrics", BasicAuth(stats.MakePrometheusHandler()))

	router.HandleFunc("/api/v0.1/statistics", BasicAuth(apiStatistics))
	router.HandleFunc("/api/v0.1/torrents", BasicAuth(apiTorrents))
	router.HandleFunc("/api/v0.1/torrentstotal", BasicAuth(apiTorrentsTotal))
	router.HandleFunc("/api/v0.1/torrents/{infohash}", BasicAuth(infohashMiddleware(apiTorrent)))
	router.HandleFunc("/api/v0.1/torrents/{infohash}/filelist", BasicAuth(infohashMiddleware(apiFileList)))

	router.HandleFunc("/feed", BasicAuth(feedHandler))
	router.HandleFunc("/statistics", BasicAuth(statisticsHandler))
	router.HandleFunc("/torrents/", BasicAuth(torrentsInfohashHandler))
	router.HandleFunc("/torrents", BasicAuth(torrentsHandler))

	return router
}

package rest

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/OrlovEvgeny/Lynxdb/pkg/alerts"
)

func (s *Server) handleCreateAlert(w http.ResponseWriter, r *http.Request) {
	var input alerts.AlertInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}

	alert, err := s.alertMgr.Create(&input)
	if err != nil {
		switch {
		case errors.Is(err, alerts.ErrAlertAlreadyExists):
			respondError(w, ErrCodeAlreadyExists, http.StatusConflict, err.Error())
		case errors.Is(err, alerts.ErrAlertNameEmpty), errors.Is(err, alerts.ErrQueryEmpty), errors.Is(err, alerts.ErrNoChannels), errors.Is(err, alerts.ErrUnknownChannelType):
			respondError(w, ErrCodeValidationError, http.StatusUnprocessableEntity, err.Error())
		case errors.Is(err, alerts.ErrInvalidInterval):
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		default:
			respondError(w, ErrCodeValidationError, http.StatusUnprocessableEntity, err.Error())
		}

		return
	}

	respondData(w, http.StatusCreated, alert)
}

func (s *Server) handleListAlerts(w http.ResponseWriter, r *http.Request) {
	list := s.alertMgr.List()
	respondData(w, http.StatusOK, map[string]interface{}{"alerts": list})
}

func (s *Server) handleGetAlert(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	alert, err := s.alertMgr.Get(id)
	if err != nil {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())

		return
	}
	respondData(w, http.StatusOK, alert)
}

func (s *Server) handleUpdateAlert(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var input alerts.AlertInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}

	alert, err := s.alertMgr.Update(id, &input)
	if err != nil {
		switch {
		case errors.Is(err, alerts.ErrAlertNotFound):
			respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())
		case errors.Is(err, alerts.ErrAlertNameEmpty), errors.Is(err, alerts.ErrQueryEmpty), errors.Is(err, alerts.ErrNoChannels), errors.Is(err, alerts.ErrUnknownChannelType):
			respondError(w, ErrCodeValidationError, http.StatusUnprocessableEntity, err.Error())
		case errors.Is(err, alerts.ErrInvalidInterval):
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		default:
			respondError(w, ErrCodeValidationError, http.StatusUnprocessableEntity, err.Error())
		}

		return
	}

	respondData(w, http.StatusOK, alert)
}

func (s *Server) handleDeleteAlert(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.alertMgr.Delete(id); err != nil {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleTestAlert(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := s.alertMgr.TestAlert(r.Context(), id)
	if err != nil {
		if errors.Is(err, alerts.ErrAlertNotFound) {
			respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())
		} else {
			respondInternalError(w, err.Error())
		}

		return
	}
	respondData(w, http.StatusOK, result)
}

func (s *Server) handleTestAlertChannels(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	results, err := s.alertMgr.TestChannels(r.Context(), id)
	if err != nil {
		if errors.Is(err, alerts.ErrAlertNotFound) {
			respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())
		} else {
			respondInternalError(w, err.Error())
		}

		return
	}
	respondData(w, http.StatusOK, map[string]interface{}{"results": results})
}

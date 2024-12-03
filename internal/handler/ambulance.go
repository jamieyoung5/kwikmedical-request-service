package handler

import (
	"encoding/json"
	cloudeventspb "github.com/jamieyoung5/kwikmedical-eventstream/pb/io/cloudevents/v1"
	"go.uber.org/zap"
)

func (h *Handler) processCompleteAmbulanceEvent(event *cloudeventspb.CloudEvent) error {
	ambulanceRequest, err := unmarshalEventToAmbulanceRequest(event)
	if err != nil {
		h.logger.Error("error while unmarshaling event", zap.Error(err), zap.String("Id", event.Id))
		return err
	}

	err = h.dbClient.UnassignAmbulance(int(ambulanceRequest.RequestId))
	if err != nil {
		h.logger.Error("error while unassigning ambulance request", zap.Error(err), zap.String("Id", event.Id))
		return err
	}

	context := map[string]int{
		"contextId": int(ambulanceRequest.HospitalId),
	}
	contextJsonBytes, err := json.Marshal(context)
	if err != nil {
		h.logger.Error("error marshalling context", zap.Error(err))
		return err
	}

	return h.publishAmbulanceEvent(
		ambulanceRequest,
		string(ambulanceRequest.EmergencyCallId),
		"UpdatedAmbulanceRequest",
		contextJsonBytes,
	)
}

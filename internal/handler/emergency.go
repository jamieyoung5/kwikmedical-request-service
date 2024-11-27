package handler

import (
	"encoding/json"
	pbSchema "github.com/jamieyoung5/kwikmedical-eventstream/pb"
	cloudeventspb "github.com/jamieyoung5/kwikmedical-eventstream/pb/io/cloudevents/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func (h *Handler) processEmergencyEvent(event *cloudeventspb.CloudEvent) error {
	var emergency pbSchema.AmbulanceRequest
	err := proto.Unmarshal(event.GetProtoData().GetValue(), &emergency)
	if err != nil {
		h.logger.Error("Failed to unmarshal EmergencyCall", zap.Error(err))
	}

	reqId, err := h.dbClient.CreateNewAmbulanceRequest(&emergency)
	if err != nil {
		h.logger.Error("Failed to create new AmbulanceRequest", zap.Error(err))
		return err
	}
	emergency.RequestId = reqId

	ambulanceId, err := h.dbClient.AssignAmbulance(int(reqId))
	if err != nil {
		h.logger.Error("Failed to assign AmbulanceRequest", zap.Error(err))
		return err
	}

	err = h.publishAmbulanceEvent(&emergency, string(emergency.EmergencyCallId), "AmbulanceRequestCreated", nil)
	if err != nil {
		h.logger.Error("Failed to publish AmbulanceRequest", zap.Error(err))
		return err
	}

	if ambulanceId != nil {
		context := map[string]int{
			"ambulanceId": int(*ambulanceId),
		}
		contextJsonBytes, marshalErr := json.Marshal(context)
		if marshalErr != nil {
			h.logger.Error("error marshalling context", zap.Error(err))
			return marshalErr
		}

		err = h.publishAmbulanceEvent(&emergency, string(emergency.EmergencyCallId), "AmbulanceAssigned", contextJsonBytes)
		if err != nil {
			h.logger.Error("Failed to publish AmbulanceAssigned", zap.Error(err))
			return err
		}
	} else {
		h.logger.Debug("unable to assign ambulance")
	}

	return nil
}

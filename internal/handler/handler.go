package handler

import (
	"github.com/jamieyoung5/kwikmedical-db-lib/pkg/client"
	pbSchema "github.com/jamieyoung5/kwikmedical-eventstream/pb"
	cloudeventspb "github.com/jamieyoung5/kwikmedical-eventstream/pb/io/cloudevents/v1"
	"github.com/jamieyoung5/kwikmedical-eventstream/pkg/eventutil"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Handler struct {
	dbClient    *client.KwikMedicalDBClient
	eventStream pbSchema.EventStreamV1Client
	logger      *zap.Logger
}

func NewHandler(logger *zap.Logger, eventStream pbSchema.EventStreamV1Client, dbClient *client.KwikMedicalDBClient) *Handler {
	return &Handler{
		dbClient:    dbClient,
		eventStream: eventStream,
		logger:      logger,
	}
}

func (h *Handler) ProcessEvents() error {
	subRequest := &pbSchema.SubscriptionRequest{
		Types: []string{"AmbulanceRequest", "CompleteAmbulanceRequest"},
	}

	return eventutil.Consume(
		h.eventStream,
		h.logger,
		subRequest,
		h.handleEvent)
}

func (h *Handler) handleEvent(event *cloudeventspb.CloudEvent) {
	h.logger.Debug("Received event",
		zap.String("Id", event.Id),
		zap.String("Type", event.Type),
		zap.String("Source", event.Source),
	)

	switch event.Type {
	case "AmbulanceRequest":
		err := h.processEmergencyEvent(event)
		if err != nil {
			h.logger.Error("error while processing emergency event", zap.Error(err), zap.String("Id", event.Id))
			return
		}
	case "CompleteAmbulanceRequest":
		err := h.processCompleteAmbulanceEvent(event)
		if err != nil {
			h.logger.Error("error while processing complete event", zap.Error(err), zap.String("Id", event.Id))
			return
		}

	default:
		h.logger.Warn("Unhandled event type", zap.String("event type", event.Type))
	}
}

func (h *Handler) publishAmbulanceEvent(request proto.Message, id string, eventType string, context []byte) error {
	jsonBytes, err := protojson.Marshal(request)
	if err != nil {
		h.logger.Error("Error marshalling event", zap.Error(err))
		return err
	}

	reqEvent := &cloudeventspb.CloudEvent{
		Id:          id,
		Source:      "kwikmedical-request-service",
		SpecVersion: "1.0",
		Type:        eventType,
		Data: &cloudeventspb.CloudEvent_TextData{
			TextData: string(jsonBytes),
		},
	}

	if context != nil {
		reqEvent.Attributes = map[string]*cloudeventspb.CloudEvent_CloudEventAttributeValue{
			"context": {Attr: &cloudeventspb.CloudEvent_CloudEventAttributeValue_CeBytes{CeBytes: context}},
		}
	}

	_, err = eventutil.Publish(h.eventStream, reqEvent, h.logger)
	if err != nil {
		h.logger.Error("Failed to publish a new Ambulance Request", zap.Error(err))
		return err
	}

	return nil
}

func unmarshalEventToAmbulanceRequest(event *cloudeventspb.CloudEvent) (*pbSchema.AmbulanceRequest, error) {
	var emergency pbSchema.AmbulanceRequest
	err := proto.Unmarshal(event.GetProtoData().GetValue(), &emergency)

	return &emergency, err
}

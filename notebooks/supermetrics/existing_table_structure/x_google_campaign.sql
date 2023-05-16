SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T020_GoogleCampaign](
        [C020_QueryName] [varchar](50) NULL,
        [C020_Date] [date] NULL,
        [C020_Year] [varchar](255) NULL,
        [C020_YearMonth] [varchar](255) NULL,
        [C020_Month] [varchar](255) NULL,
        [C020_DayofMonth] [varchar](255) NULL,
        [C020_DayofWeek] [varchar](255) NULL,
        [C020_CampaignName] [varchar](255) NULL,
        [C020_CampaignID] [varchar](255) NULL,
        [C020_Account] [varchar](255) NULL,
        [C020_AccountID] [varchar](255) NULL,
        [C020_Network] [varchar](255) NULL,
        [C020_CampaignStatus] [varchar](255) NULL,
        [C020_ServingStatus] [varchar](255) NULL,
        [C020_AdvertisingChannelType] [varchar](255) NULL,
        [C020_AdvertisingChannelSubType] [varchar](255) NULL,
        [C020_StartDate] [varchar](255) NULL,
        [C020_EndDate] [varchar](255) NULL,
        [C020_BiddingStrategy] [varchar](255) NULL,
        [C020_CampaignDesktopBidModifier] [varchar](255) NULL,
        [C020_CampaignMobileBidModifier] [varchar](255) NULL,
        [C020_CampaignTabletBidModifier] [varchar](255) NULL,
        [C020_CampaignLabels] [varchar](255) NULL,
        [C020_Impressions] [float] NULL,
        [C020_Clicks] [float] NULL,
        [C020_Cost] [float] NULL,
        [C020_Ctr] [varchar](255) NULL,
        [C020_Cpc] [float] NULL,
        [C020_Cpm] [float] NULL,
        [C020_Budget] [float] NULL,
        [C020_Conversions] [float] NULL,
        [C020_ConversionRate] [varchar](255) NULL,
        [C020_ConversionsPerImpression] [varchar](255) NULL,
        [C020_CostPerConversion] [float] NULL,
        [C020_ValuePerConversion] [float] NULL,
        [C020_TotalConversionValue] [float] NULL,
        [C020_ReturnOnAdSpend] [float] NULL,
        [C020_ViewThroughConversions] [float] NULL,
        [C020_AllConversions] [float] NULL,
        [C020_AllConversionValue] [float] NULL,
        [C020_VideoImpressions] [float] NULL,
        [C020_VideoViews] [float] NULL,
        [C020_PhoneCallImpressions] [float] NULL,
        [C020_Calls] [float] NULL,
        [C020_CallRate] [varchar](255) NULL,
        [C020_GmailForwards] [float] NULL,
        [C020_GmailSaves] [float] NULL,
        [C020_GmailSecondaryClicks] [float] NULL,
        [C020_ImpressionShare] [float] NULL,
        [C020_SearchImpressionShare] [varchar](255) NULL,
        [C020_AvailableImpressions] [float] NULL,
        [C020_TopImpressionPercentage] [varchar](255) NULL,
        [C020_AbsoluteTopImpressionPercentage] [varchar](255) NULL,
        [C020_SearchAbsoluteTopImpressionShare] [varchar](255) NULL,
        [C020_SearchTopImpressionShare] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL,
        [C020_CampaignVerticle] [char](3) NULL
    ) ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T020_GoogleCampaign]
ADD
    CONSTRAINT [DF__T020_Goog__DateR__5B78929E] DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO
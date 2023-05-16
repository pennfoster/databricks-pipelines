SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T011_GoogleDevice](
        [C011_QueryName] [varchar](50) NULL,
        [C011_Date] [date] NULL,
        [C011_Year] [varchar](255) NULL,
        [C011_YearMonth] [varchar](255) NULL,
        [C011_Month] [varchar](255) NULL,
        [C011_DayofMonth] [varchar](255) NULL,
        [C011_DayofWeek] [varchar](255) NULL,
        [C011_Device] [varchar](255) NULL,
        [C011_Keyword] [varchar](255) NULL,
        [C011_KeywordID] [varchar](255) NULL,
        [C011_AdId] [varchar](255) NULL,
        [C011_AdGroupName] [varchar](255) NULL,
        [C011_AdGroupID] [varchar](255) NULL,
        [C011_CampaignName] [varchar](255) NULL,
        [C011_CampaignID] [varchar](255) NULL,
        [C011_Account] [varchar](255) NULL,
        [C011_AccountID] [varchar](255) NULL,
        [C011_TimeZone] [varchar](255) NULL,
        [C011_Network] [varchar](255) NULL,
        [C011_NetworkWithSearchPartners] [varchar](255) NULL,
        [C011_CampaignStatus] [varchar](255) NULL,
        [C011_AdGroupStatus] [varchar](255) NULL,
        [C011_FinalUrl] [varchar](max) NULL,
        [C011_TrackingUrlTemplate] [varchar](max) NULL,
        [C011_Impressions] [float] NULL,
        [C011_Clicks] [float] NULL,
        [C011_Cost] [float] NULL,
        [C011_Ctr] [varchar](255) NULL,
        [C011_Cpc] [float] NULL,
        [C011_Cpm] [float] NULL,
        [C011_AveragePosition] [float] NULL,
        [C011_Conversions] [float] NULL,
        [C011_ConversionRate] [varchar](255) NULL,
        [C011_ConversionsPerImpression] [varchar](255) NULL,
        [C011_CostPerConversion] [float] NULL,
        [C011_ValuePerConversion] [float] NULL,
        [C011_TotalConversionValue] [float] NULL,
        [C011_ReturnOnAdSpend] [float] NULL,
        [C011_ViewThroughConversions] [float] NULL,
        [C011_AllConversions] [float] NULL,
        [C011_AllConversionValue] [float] NULL,
        [C011_VideoImpressions] [float] NULL,
        [C011_VideoViews] [float] NULL,
        [C011_VideoViewRate] [varchar](255) NULL,
        [C011_Watch25PercentRate] [varchar](255) NULL,
        [C011_Watch50PercentRate] [varchar](255) NULL,
        [C011_Watch75PercentRate] [varchar](255) NULL,
        [C011_Watch100PercentRate] [varchar](255) NULL,
        [C011_Watch25PercentViews] [float] NULL,
        [C011_Watch50PercentViews] [float] NULL,
        [C011_Watch75PercentViews] [float] NULL,
        [C011_Watch100PercentViews] [float] NULL,
        [C011_TopImpressionPercentage] [varchar](255) NULL,
        [C011_AbsoluteTopImpressionPercentage] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T011_GoogleDevice]
ADD
    DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO
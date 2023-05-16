SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T006_BingSearchAds](
        [C006_QueryName] [varchar](50) NULL,
        [C006_Date] [date] NULL,
        [C006_Year] [varchar](255) NULL,
        [C006_YearMonth] [varchar](255) NULL,
        [C006_Month] [varchar](255) NULL,
        [C006_DayofMonth] [varchar](255) NULL,
        [C006_DayofWeek] [varchar](255) NULL,
        [C006_AdID] [varchar](255) NULL,
        [C006_CustomParameters] [varchar](255) NULL,
        [C006_AccountName] [varchar](255) NULL,
        [C006_AccountID] [varchar](255) NULL,
        [C006_AccountNumber] [varchar](255) NULL,
        [C006_CampaignName] [varchar](255) NULL,
        [C006_CampaignID] [varchar](255) NULL,
        [C006_Status] [varchar](255) NULL,
        [C006_AdGroupName] [varchar](255) NULL,
        [C006_AdGroupID] [varchar](255) NULL,
        [C006_AdGroupStatus] [varchar](255) NULL,
        [C006_AdTitle] [varchar](255) NULL,
        [C006_AdTitlePart1] [varchar](255) NULL,
        [C006_AdTitlePart2] [varchar](255) NULL,
        [C006_AdTitlePart3] [varchar](255) NULL,
        [C006_AdPathPart1] [varchar](255) NULL,
        [C006_AdPathPart2] [varchar](255) NULL,
        [C006_AdDescription] [varchar](255) NULL,
        [C006_AdDescription2] [varchar](255) NULL,
        [C006_AdType] [varchar](255) NULL,
        [C006_AdStatus] [varchar](255) NULL,
        [C006_FinalURL] [varchar](max) NULL,
        [C006_DisplayURL] [varchar](max) NULL,
        [C006_TrackingTemplate] [varchar](255) NULL,
        [C006_CampaignLabels] [varchar](255) NULL,
        [C006_AdGroupLabels] [varchar](255) NULL,
        [C006_AdLabels] [varchar](255) NULL,
        [C006_Impressions] [float] NULL,
        [C006_Clicks] [float] NULL,
        [C006_Cost] [float] NULL,
        [C006_CTRPercentage] [float] NULL,
        [C006_CPC] [float] NULL,
        [C006_CPM] [float] NULL,
        [C006_AveragePosition] [float] NULL,
        [C006_Conversions] [float] NULL,
        [C006_ConversionRate] [varchar](255) NULL,
        [C006_ConversionsPerImpressionRate] [varchar](255) NULL,
        [C006_CostPerConversion] [float] NULL,
        [C006_RevenuePerConversion] [float] NULL,
        [C006_Revenue] [float] NULL,
        [C006_ReturnOnAdSpend] [float] NULL,
        [C006_Mkwid] [varchar](255) NULL,
        [C006_Pubcode] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL,
        [C006_Adkey] [varchar](255) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T006_BingSearchAds]
ADD
    DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO
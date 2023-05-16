SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T021_GoogleKeyword_labels](
        [C021_QueryName] [varchar](50) NULL,
        [C021_Date] [date] NULL,
        [C021_Keyword] [varchar](255) NULL,
        [C021_CustomURLParameters] [varchar](255) NULL,
        [C021_Year] [varchar](255) NULL,
        [C021_YearMonth] [varchar](255) NULL,
        [C021_Month] [varchar](255) NULL,
        [C021_DayofMonth] [varchar](255) NULL,
        [C021_DayofWeek] [varchar](255) NULL,
        [C021_Account] [varchar](255) NULL,
        [C021_AccountID] [varchar](255) NULL,
        [C021_CampaignName] [varchar](255) NULL,
        [C021_CampaignID] [varchar](255) NULL,
        [C021_CampaignStatus] [varchar](255) NULL,
        [C021_BiddingStrategy] [varchar](255) NULL,
        [C021_AdGroupName] [varchar](255) NULL,
        [C021_AdGroupID] [varchar](255) NULL,
        [C021_AdGroupStatus] [varchar](255) NULL,
        [C021_FinalURL] [varchar](max) NULL,
        [C021_FinalMobileURL] [varchar](max) NULL,
        [C021_TrackingURLTemplate] [varchar](max) NULL,
        [C021_MatchType] [varchar](255) NULL,
        [C021_KeywordID] [varchar](255) NULL,
        [C021_KeywordStatus] [varchar](255) NULL,
        [C021_QualityScore] [varchar](255) NULL,
        [C021_FirstpageCPC] [varchar](255) NULL,
        [C021_TopofPageCPC] [varchar](255) NULL,
        [C021_FirstpositionCPC] [varchar](255) NULL,
        [C021_Impressions] [varchar](255) NULL,
        [C021_Clicks] [varchar](255) NULL,
        [C021_Cost] [varchar](255) NULL,
        [C021_CTR] [varchar](255) NULL,
        [C021_CPC] [varchar](255) NULL,
        [C021_CPM] [varchar](255) NULL,
        [C021_Conversions] [varchar](255) NULL,
        [C021_ConversionRate] [varchar](255) NULL,
        [C021_ConversionsperImpression] [varchar](255) NULL,
        [C021_CostperConversion] [varchar](255) NULL,
        [C021_ValueperConversion] [varchar](255) NULL,
        [C021_TotalConversionValue] [varchar](255) NULL,
        [C021_ReturnOnAdSpend] [varchar](255) NULL,
        [C021_ViewThroughconversions] [varchar](255) NULL,
        [C021_AllConversions] [varchar](255) NULL,
        [C021_AllConversionValue] [varchar](255) NULL,
        [C021_ImpressionShare] [varchar](255) NULL,
        [C021_SearchImpressionShare] [varchar](255) NULL,
        [C021_AvailableImpressions] [varchar](255) NULL,
        [C021_TopImpressionPercentage] [varchar](255) NULL,
        [C021_AbsoluteTopImpressionPercentage] [varchar](255) NULL,
        [C021_SearchAbsoluteTopImpressionShare] [varchar](255) NULL,
        [C021_SearchTopImpressionShare] [varchar](255) NULL,
        [C021_Mkwid] [varchar](255) NULL,
        [C021_Pubcode] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NOT NULL,
        [C021_Adkey] [varchar](255) NULL,
        [C021_LandingPageURL] [varchar](max) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T021_GoogleKeyword_labels]
ADD
    DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO